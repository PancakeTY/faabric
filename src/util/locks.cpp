#include <faabric/util/locks.h>

namespace faabric::util {

FlagWaiter::FlagWaiter(int timeoutMsIn)
  : timeoutMs(timeoutMsIn)
{}

void FlagWaiter::waitOnFlag()
{
    // Keep the this shared_ptr alive to prevent heap-use-after-free
    std::shared_ptr<FlagWaiter> _keepMeAlive = shared_from_this();
    // Check
    if (flag.load()) {
        return;
    }

    // Wait for flag to be set
    UniqueLock lock(flagMx);
    if (!cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this] {
            return flag.load();
        })) {

        SPDLOG_ERROR("Timed out waiting for flag");
        throw std::runtime_error("Timed out waiting for flag");
    }
}

void FlagWaiter::setFlag(bool value)
{
    // Keep the this shared_ptr alive to prevent heap-use-after-free
    std::shared_ptr<FlagWaiter> _keepMeAlive = shared_from_this();
    UniqueLock lock(flagMx);
    flag.store(value);
    cv.notify_all();
}

void IndivLock::acquire()
{
    std::unique_lock<std::mutex> lk(mtx);
    if (locked) {
        // Add the thread to the waiting queue and wait until notified
        waiting_threads.push(std::this_thread::get_id());
        cv.wait(lk, [this]() {
            return !locked &&
                   waiting_threads.front() == std::this_thread::get_id();
        });
        waiting_threads.pop();
    }
    // Lock is acquired
    locked = true;
}

void IndivLock::release()
{
    std::unique_lock<std::mutex> lk(mtx);
    locked = false;
    if (!waiting_threads.empty()) {
        // Notify the next waiting thread
        cv.notify_all();
    }
}

bool IndivLock::isLocked()
{
    std::lock_guard<std::mutex> lk(mtx);
    return locked;
}

RangeLock::RangeLock(int begin, int end)
{
    rangeMin = begin;
    rangeMax = end;
    createVersion(1, { { begin, end } });
}

bool RangeLock::areSubRangesContinuous(const std::map<int, int>& ranges,
                                       int min,
                                       int max) const
{
    int currentMin = min;

    for (const auto& [begin, end] : ranges) {
        if (begin != currentMin) {
            return false;
        }
        currentMin = end + 1;
    }

    return currentMin == max + 1;
}

void RangeLock::createVersion(int version, const std::map<int, int>& ranges)
{
    std::lock_guard<std::mutex> lock(globalMutex);

    if (version != currentVersion + 1) {
        throw std::runtime_error("Invalid version version number");
    }

    currentVersion = version;
    // If the version if greater than one. We should check the range of the new
    // version is the same as the first version. We also have to check the
    // sub-ranges are continuous.
    if (version > 1) {
        if (!areSubRangesContinuous(ranges, rangeMin, rangeMax)) {
            SPDLOG_ERROR("Sub-ranges are not continuous.");
            throw std::runtime_error(
              "Partitioned State new Sub-ranges are not continuous");
            return;
        }
    }

    for (const auto& [begin, end] : ranges) {
        versionLocks[version].try_emplace(std::make_pair(begin, end));
    }

    // Delete the outdated version. We only keep two versions of locks.
    if (version > 2) {
        versionLocks.erase(version - 2);
    }
}

bool overlap(const std::pair<int, int>& a, const std::pair<int, int>& b)
{
    return !(a.second < b.first || a.first > b.second);
}

bool RangeLock::canAcquireLock(int version, int begin, int end)
{
    auto rangePair = std::make_pair(begin, end);
    // Check if the overlap locks in another version are locked.
    if (version == currentVersion && version > 1) {
        for (const auto& [prevRange, prevLock] : versionLocks[version - 1]) {
            if (overlap(rangePair, prevRange) && prevLock.isLocked) {
                return false;
            }
        }
    } else if (version == currentVersion - 1) {
        for (const auto& [currRange, currLock] : versionLocks[version + 1]) {
            if (overlap(rangePair, currRange) && currLock.isLocked) {
                return false;
            }
        }
    }

    auto& rangeLock = versionLocks[version].at(rangePair);
    if (rangeLock.isLocked) {
        return false;
    }

    return true;
}

// It is allowed to have two versions of locks at the same time. In this
// case, lower version has higher priority. If range of the new version
// overlap with the previous version, it should wait until the previous
// version is released.
// Vise Versa
void RangeLock::acquire(int version, int begin, int end)
{
    std::unique_lock<std::mutex> globalLock(globalMutex);
    auto timestamp = std::chrono::steady_clock::now();

    if (!versionLocks.contains(version) ||
        !versionLocks[version].contains(std::make_pair(begin, end))) {
        SPDLOG_ERROR("Try to gain lock for non-existing version: {} : {}-{}",
                     version,
                     begin,
                     end);
        throw std::runtime_error("Invalid version or range");
    }

    auto& rangeLock = versionLocks[version].at(std::make_pair(begin, end));
    std::unique_lock<std::mutex> rangeLockGuard(rangeLock.mtx);
    // If cannoe aquire the lock, wait until it can.
    while (!canAcquireLock(version, begin, end)) {
        std::condition_variable cv;
        Range::WaitInfo waitInfo = { timestamp, &cv };
        rangeLock.waitQueue.push(waitInfo);
        globalLock.unlock();
        cv.wait(rangeLockGuard);
        globalLock.lock();
    }

    rangeLock.isLocked = true;
}

void RangeLock::release(int version, int begin, int end)
{
    std::unique_lock<std::mutex> globalLock(globalMutex);

    if (!versionLocks.contains(version) ||
        !versionLocks[version].contains(std::make_pair(begin, end))) {
        SPDLOG_ERROR("Try to release lock for non-existing version: {} : {}-{}",
                     version,
                     begin,
                     end);
        throw std::runtime_error("Invalid version or range");
    }

    // After release this lock, we should notify the next lock in the wait
    // queue.
    // For the old version. We have to try to notify the same lock in the
    // same version at first. If it is finished, we should try to notify the
    // overlap locks in the next version.
    // For the current version. We should check if the old version is finished.
    // If not, we should try to notify the overlap locks in the old version.
    // Otherwise, we should try to notify the same the lock in the same version
    // .

    auto& releaseRangeLock =
      versionLocks[version].at(std::make_pair(begin, end));
    std::unique_lock<std::mutex> rangeLockGuard(releaseRangeLock.mtx);
    releaseRangeLock.isLocked = false;
    if (version == currentVersion - 1) {
        if (!releaseRangeLock.waitQueue.empty()) {
            // Try to notify the next waiting thread of the same lock
            std::condition_variable* nextCv =
              releaseRangeLock.waitQueue.top().cv;
            releaseRangeLock.waitQueue.pop();
            nextCv->notify_one();
        } else {
            // For the overlap locks in the next version, notify one for each.
            for (auto& [currRange, currLock] : versionLocks[version + 1]) {
                if (overlap(std::pair(begin, end), currRange)) {
                    if (currLock.isLocked) {
                        SPDLOG_ERROR("The overlap locks are locked both");
                        throw std::runtime_error(
                          "The overlap locks are locked both");
                    }
                    if (!currLock.waitQueue.empty()) {
                        std::unique_lock<std::mutex> tempLockGurad(
                          currLock.mtx);
                        auto nextCv = currLock.waitQueue.top().cv;
                        currLock.waitQueue.pop();
                        nextCv->notify_one();
                    }
                }
            }
        }
    } else if (version == currentVersion) {
        bool unlockPrev = false;
        // If the old version is not finished, we should try to notify the
        // overlap locks in the old version.
        if (version != 1) {
            // For the overlap locks in the previous version, notify one for
            // each.
            for (auto& [prevRange, prevLock] : versionLocks[version - 1]) {
                if (overlap(std::pair(begin, end), prevRange)) {
                    if (prevLock.isLocked) {
                        SPDLOG_ERROR("The overlap locks are locked both");
                        throw std::runtime_error(
                          "The overlap locks are locked both");
                    }
                    if (!prevLock.waitQueue.empty()) {
                        std::unique_lock<std::mutex> tempLockGurad(
                          prevLock.mtx);
                        auto nextCv = prevLock.waitQueue.top().cv;
                        prevLock.waitQueue.pop();
                        nextCv->notify_one();
                        unlockPrev = true;
                    }
                }
            }
        }
        if (!unlockPrev) {
            if (!releaseRangeLock.waitQueue.empty()) {
                // Try to notify the next waiting thread of the same lock
                std::condition_variable* nextCv =
                  releaseRangeLock.waitQueue.top().cv;
                releaseRangeLock.waitQueue.pop();
                nextCv->notify_one();
            }
        }
    } else {
        SPDLOG_ERROR("Try to gain lock for aquired version: {} and Current "
                     "version: {}",
                     version,
                     currentVersion);
        throw std::runtime_error("Invalid version");
    }
}

// Implementation of the getInfo function
RangeLock::LockInfo RangeLock::getInfo() const
{
    LockInfo info;
    info.currentVersion = currentVersion;
    info.rangeMin = rangeMin;
    info.rangeMax = rangeMax;
    return info;
}

}
