#pragma once

#include <faabric/util/logging.h>

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <thread>
#include <vector>

#define DEFAULT_FLAG_WAIT_MS 10000

namespace faabric::util {
typedef std::unique_lock<std::mutex> UniqueLock;
typedef std::unique_lock<std::shared_mutex> FullLock;
typedef std::shared_lock<std::shared_mutex> SharedLock;

class FlagWaiter : public std::enable_shared_from_this<FlagWaiter>
{
  public:
    FlagWaiter(int timeoutMsIn = DEFAULT_FLAG_WAIT_MS);

    void waitOnFlag();

    void setFlag(bool value);

  private:
    int timeoutMs;

    std::mutex flagMx;
    std::condition_variable cv;
    std::atomic<bool> flag;
};

/***
 * This Lock is used by Paritioned State to partition the state into ranges and
 * only lock the required ranges.
 ***/

class RangeLock
{
  public:
    RangeLock(int begin, int end);
    void createVersion(int version, const std::map<int, int>& ranges);
    void acquire(int version, int begin, int end);
    void release(int version, int begin, int end);

    struct LockInfo
    {
        int currentVersion;
        int rangeMin;
        int rangeMax;
    };

    LockInfo getInfo() const;

  private:
    struct Range
    {
        std::mutex mtx;
        bool isLocked;

        struct WaitInfo
        {
            std::chrono::steady_clock::time_point timestamp;
            std::condition_variable* cv;
        };

        struct Compare
        {
            bool operator()(const WaitInfo& a, const WaitInfo& b)
            {
                // Earlier timestamp has higher priority
                return a.timestamp > b.timestamp;
            }
        };

        std::priority_queue<WaitInfo, std::vector<WaitInfo>, Compare> waitQueue;
        Range()
          : isLocked(false)
        {}
        Range(const Range&) = delete;
        Range& operator=(const Range&) = delete;
    };

    int rangeMin;
    int rangeMax;
    // The first version should start from 1
    int currentVersion = 0;
    std::map<int, std::map<std::pair<int, int>, Range>> versionLocks;
    std::mutex globalMutex;

    bool areSubRangesContinuous(const std::map<int, int>& ranges,
                                int min,
                                int max) const;
    bool canAcquireLock(int version, int begin, int end);
};
}