#include <faabric/state/FunctionState.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/serialization.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/timing.h>
#include <sys/mman.h>

using namespace faabric::util;

namespace faabric::state {
FunctionState::FunctionState(const std::string& userIn,
                             const std::string& functionIn,
                             int parallelismIdIn,
                             const std::string& hostIpIn,
                             size_t stateSizeIn)
  : user(userIn)
  , function(functionIn)
  , parallelismId(parallelismIdIn)
  , stateSize(stateSizeIn)
  , stateRegistry(getFunctionStateRegistry())
  , hostIp(hostIpIn)
  , masterIp(stateRegistry.getMasterIP(userIn,
                                       functionIn,
                                       parallelismIdIn,
                                       hostIpIn,
                                       true))
{
    SPDLOG_TRACE("Creating function state for {}/{} with size {} (this "
                 "parallelisimId: {})",
                 user,
                 function,
                 stateSize,
                 parallelismId);
    if (hostIp == masterIp) {
        isMaster = true;
    }
    // If stateSizeIn is not set, the configure has to be called later.
    if (stateSizeIn > 0) {
        configureSize();
    }
}

FunctionState::FunctionState(const std::string& userIn,
                             const std::string& functionIn,
                             int parallelismIdIn,
                             const std::string& hostIpIn)
  : FunctionState(userIn, functionIn, parallelismIdIn, hostIpIn, 0)
{}

long FunctionState::lockWrite()
{
    long startTime = faabric::util::getGlobalClock().epochMillis();
    SPDLOG_TRACE(
      "Waiting Locking write for {}/{}-{}", user, function, parallelismId);
    stateMutex.lock();
    SPDLOG_TRACE(
      "Gain Locked write for {}/{}-{}", user, function, parallelismId);
    long endTime = faabric::util::getGlobalClock().epochMillis();
    return endTime - startTime;
}

void FunctionState::unlockWrite()
{
    stateMutex.unlock();
}

long FunctionState::lockMasterWrite()
{
    if (isMaster) {
        return lockWrite();
    }
    FunctionStateClient cli(user, function, parallelismId, masterIp);
    cli.lock();
}
void FunctionState::unlockMasterWrite()
{
    if (isMaster) {
        return unlockWrite();
    }
    FunctionStateClient cli(user, function, parallelismId, masterIp);
    cli.unlock();
}

void FunctionState::checkSizeConfigured()
{
    if (stateSize <= 0) {
        throw FunctionStateException(
          fmt::format("{}/{} has no size set", user, function));
    }
}

void FunctionState::configureSize()
{
    // Work out size of required shared memory
    size_t nHostPages = getRequiredHostPages(stateSize);
    sharedMemSize = nHostPages * HOST_PAGE_SIZE;
    sharedMemory = nullptr;
}

size_t FunctionState::size() const
{
    return stateSize;
}

void FunctionState::setPartitionKey(std::string key)
{
    partitionKey = key;
}

void FunctionState::clearAll(bool global)
{
    FunctionStateRegistry& reg = state::getFunctionStateRegistry();
    reg.clear();
}

uint32_t FunctionState::waitOnRedisRemoteLockFunc(const std::string& redisKey)
{
    PROF_START(remoteLock)

    redis::Redis& redis = redis::Redis::getState();
    uint32_t remoteLockId =
      redis.acquireLock(redisKey, REMOTE_LOCK_TIMEOUT_SECS);
    unsigned int retryCount = 0;
    while (remoteLockId == 0) {
        SPDLOG_DEBUG(
          "Waiting on remote lock for {} (loop {})", redisKey, retryCount);

        if (retryCount >= REMOTE_LOCK_MAX_RETRIES) {
            SPDLOG_ERROR("Timed out waiting for lock on {}", redisKey);
            break;
        }

        SLEEP_MS(500);

        remoteLockId = redis.acquireLock(redisKey, REMOTE_LOCK_TIMEOUT_SECS);
        retryCount++;
    }

    PROF_END(remoteLock)
    return remoteLockId;
}

void FunctionState::set(const uint8_t* buffer)
{
    checkSizeConfigured();

    doSet(buffer);
}

void FunctionState::reSize(long length)
{
    stateSize = length;
    checkSizeConfigured();
    // If new length is bigger than the reserved size, reallocate it.
    if (length > sharedMemSize) {
        fullyAllocated = false;
        SPDLOG_DEBUG(
          "The new length is bigger than the reserved size, reallocate it.");
        // If the sharedMemory is created, but not initialized, the shared
        // memory is null. In FunctionState, sizeless intialize is not allowed.
        if (sharedMemory != nullptr) {
            if (munmap(sharedMemory, sharedMemSize) == -1) {
                SPDLOG_ERROR("Failed to unmap shared memory: {}",
                             strerror(errno));
                throw std::runtime_error("Failed unmapping memory for FS");
            }
            sharedMemory = nullptr;
        }
        configureSize();
    }
    allocateChunk(0, sharedMemSize);
}

void FunctionState::set(const uint8_t* buffer, long length, bool unlock)
{
    reSize(length);
    doSet(buffer);
    if (!isMaster) {
        pushToRemote(unlock);
        return;
    }
    if (unlock) {
        unlockWrite();
    }
}

void FunctionState::reserveStorage()
{
    checkSizeConfigured();

    // Check if already reserved
    if (sharedMemory != nullptr) {
        return;
    }

    PROF_START(reserveStorage)

    if (sharedMemSize == 0) {
        throw FunctionStateException("Reserving storage with no size for " +
                                     function);
    }

    // Create shared memory region with no permissions
    sharedMemory = mmap(
      nullptr, sharedMemSize, PROT_NONE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sharedMemory == MAP_FAILED) {
        SPDLOG_DEBUG("Mmapping of storage size {} failed. errno: {}",
                     sharedMemSize,
                     errno);

        throw std::runtime_error("Failed mapping memory for FS");
    }

    SPDLOG_DEBUG("Reserved {} pages of shared storage for {}",
                 sharedMemSize / HOST_PAGE_SIZE,
                 function);

    PROF_END(reserveStorage)
}

void FunctionState::allocateChunk(long offset, size_t length)
{
    // Can skip if the whole thing is already allocated
    if (fullyAllocated) {
        return;
    }

    // Ensure storage is reserved
    reserveStorage();

    // Page-align the chunk
    AlignedChunk chunk = getPageAlignedChunk(offset, length);

    // Make sure all the pages involved are writable
    int res = mprotect(
      BYTES(sharedMemory) + chunk.nBytesOffset, chunk.nBytesLength, PROT_WRITE);
    if (res != 0) {
        SPDLOG_DEBUG(
          "Allocating memory for {}/{}-{} of size {} failed: {} ({})",
          user,
          function,
          parallelismId,
          length,
          errno,
          strerror(errno));
        throw std::runtime_error("Failed allocating memory for KV");
    }

    // Flag if we've now allocated the whole value
    if (offset == 0 && length == sharedMemSize) {
        fullyAllocated = true;
    }
}

std::vector<StateChunk> FunctionState::getAllChunks()
{
    // Divide the whole value up into chunks
    auto nChunks = uint32_t((stateSize + STATE_STREAMING_CHUNK_SIZE - 1) /
                            STATE_STREAMING_CHUNK_SIZE);

    std::vector<StateChunk> chunks;
    for (uint32_t i = 0; i < nChunks; i++) {
        uint32_t previousChunkEnd = i * STATE_STREAMING_CHUNK_SIZE;
        uint8_t* chunkStart = BYTES(sharedMemory) + previousChunkEnd;
        size_t chunkSize = std::min((size_t)STATE_STREAMING_CHUNK_SIZE,
                                    stateSize - previousChunkEnd);
        chunks.emplace_back(previousChunkEnd, chunkSize, chunkStart);
    }

    return chunks;
}

void FunctionState::doSet(const uint8_t* buffer)
{
    checkSizeConfigured();

    // Set up storage
    allocateChunk(0, sharedMemSize);

    // Copy data into shared region
    std::copy(buffer, buffer + stateSize, BYTES(sharedMemory));
}

void FunctionState::pushToRemote(bool unlock)
{
    if (isMaster) {
        return;
    }
    std::vector<StateChunk> allChunks = getAllChunks();
    FunctionStateClient cli(user, function, parallelismId, masterIp);
    cli.pushChunks(allChunks, stateSize, unlock, partitionKey);
}

// Only the Master node can return its data, otherwise pull at first.
void FunctionState::get(uint8_t* buffer)
{
    doPull();

    auto bytePtr = BYTES(sharedMemory);
    std::copy(bytePtr, bytePtr + stateSize, buffer);
}

uint8_t* FunctionState::get()
{
    doPull();
    return BYTES(sharedMemory);
}

void FunctionState::pull()
{
    doPull();
}

// In our function, doPull is called to retrive the data from the master node.
void FunctionState::doPull()
{
    if (isMaster) {
        return;
    }
    checkSizeConfigured();
    size_t updatedSize =
      getStateSizeFromRemote(user, function, parallelismId, hostIp);
    // Make sure storage is allocated
    reSize(updatedSize);
    // Do the pull
    pullFromRemote();
}

void FunctionState::pullFromRemote()
{
    if (isMaster) {
        return;
    }
    std::vector<StateChunk> chunks = getAllChunks();
    FunctionStateClient cli(user, function, parallelismId, masterIp);
    cli.pullChunks(chunks, BYTES(sharedMemory));
}

void FunctionState::mapSharedMemory(void* destination,
                                    long pagesOffset,
                                    long nPages)
{
    checkSizeConfigured();

    PROF_START(mapSharedMem)

    if (!isPageAligned(destination)) {
        SPDLOG_ERROR("Non-aligned destination for shared mapping of {}",
                     function);
        throw std::runtime_error("Mapping misaligned shared memory");
    }
    // We don't have to lock there, it it locked when read the State Size.

    // Ensure the underlying memory is allocated
    size_t offset = pagesOffset * faabric::util::HOST_PAGE_SIZE;
    size_t length = nPages * faabric::util::HOST_PAGE_SIZE;
    allocateChunk(offset, length);

    // Add a mapping of the relevant pages of shared memory onto the new region
    void* result = mremap(BYTES(sharedMemory) + offset,
                          0,
                          length,
                          MREMAP_FIXED | MREMAP_MAYMOVE,
                          destination);

    // Handle failure
    if (result == MAP_FAILED) {
        SPDLOG_ERROR("Failed mapping for {} at {} with size {}. errno: {} ({})",
                     function,
                     offset,
                     length,
                     errno,
                     strerror(errno));

        throw std::runtime_error("Failed mapping shared memory");
    }

    // Check the mapping is where we expect it to be
    if (destination != result) {
        SPDLOG_ERROR("New mapped addr for {} doesn't match required {} != {}",
                     function,
                     destination,
                     result);
        throw std::runtime_error("Misaligned shared memory mapping");
    }

    PROF_END(mapSharedMem)
}

void FunctionState::unmapSharedMemory(void* mappedAddr)
{
    if (!isPageAligned(mappedAddr)) {
        SPDLOG_ERROR("Attempting to unmap non-page-aligned memory at {} for {}",
                     mappedAddr,
                     function);
        throw std::runtime_error("Unmapping misaligned shared memory");
    }

    // Unmap the current memory so it can be reused
    int result = munmap(mappedAddr, sharedMemSize);
    if (result == -1) {
        SPDLOG_ERROR(
          "Failed to unmap shared memory at {} with size {}. errno: {}",
          mappedAddr,
          sharedMemSize,
          errno);

        throw std::runtime_error("Failed unmapping shared memory");
    }
}

void FunctionState::setChunk(long offset, const uint8_t* buffer, size_t length)
{
    checkSizeConfigured();

    // Check we're in bounds - note that we permit chunks within the _allocated_
    // memory
    size_t chunkEnd = offset + length;
    if (chunkEnd > sharedMemSize) {
        SPDLOG_ERROR("Setting chunk out of bounds on {}/{}-{} ({} > {})",
                     user,
                     function,
                     parallelismId,
                     chunkEnd,
                     stateSize);
        throw std::runtime_error("Attempting to set chunk out of bounds");
    }

    // If necessary, allocate the memory
    allocateChunk(offset, length);

    // Do the copy if necessary
    if (buffer != nullptr) {
        std::copy(buffer, buffer + length, BYTES(sharedMemory) + offset);
    }
}

uint8_t* FunctionState::getChunk(long offset, long len)
{
    if (!isMaster) {
        throw FunctionStateException("Only master can get chunks");
    }
    if (offset < 0 || len < 0 || offset + len > sharedMemSize) {
        throw FunctionStateException("Requested chunk is out of bounds");
    }
    return BYTES(sharedMemory) + offset;
}

std::map<std::string, std::vector<uint8_t>> FunctionState::getStateMap()
{
    if (stateSize == 0 || sharedMemory == nullptr) {
        return std::map<std::string, std::vector<uint8_t>>();
    }
    auto bytePtr = BYTES(sharedMemory);
    std::vector<uint8_t> stateVector(bytePtr, bytePtr + stateSize);
    std::map<std::string, std::vector<uint8_t>> stateMap =
      faabric::util::deserializeFuncState(stateVector);
    return stateMap;
}

std::map<std::string, std::vector<uint8_t>> FunctionState::getParStateMap()
{
    std::map<std::string, std::vector<uint8_t>> stateMap = getStateMap();
    std::map<std::string, std::vector<uint8_t>> parStateMap =
      faabric::util::deserializeParState(stateMap[partitionKey]);
    return parStateMap;
}

bool FunctionState::rePartitionState(const std::string& newStateHost)
{
    if (!isMaster) {
        throw FunctionStateException("Only master can repartition state");
    }
    std::vector<uint8_t> tempStateVec(newStateHost.begin(), newStateHost.end());
    std::map<std::string, std::string> newStateMap =
      faabric::util::deserializeMapBinary(tempStateVec);
    // If the parallelismId is changed
    int newParallel = newStateMap.size();
    int newParallelismId = -1;
    // need to get the new parallelism Id of this host. (start from 0).
    // need to get the new parallelism Id for all the hosts.
    std::map<int, std::string> parToHost;
    std::map<std::string, int> hostToPar;
    for (auto& [userFuncParIdx, tmpHost] : newStateMap) {
        auto [tmpUser, tmpFunction, tmpParallelismId] =
          faabric::util::splitUserFuncPar(userFuncParIdx);
        // Register the new parallelism Id for all the hosts.
        parToHost[std::atoi(tmpParallelismId.c_str())] = tmpHost;
        hostToPar[tmpHost] = std::atoi(tmpParallelismId.c_str());
        if (tmpHost != hostIp) {
            continue;
        }
        // If the parallelismId is changed, record it.
        newParallelismId = std::atoi(tmpParallelismId.c_str());
    }
    // For the paritioned stateful key, reculculate their master.
    std::map<std::string, std::vector<uint8_t>> parState = getParStateMap();
    // record the partitioned key-value needed transferred to other hosts and
    // the key-value stored locally.
    std::map<std::string, std::map<std::string, std::vector<uint8_t>>>
      dataTransfer;
    std::map<std::string, std::vector<uint8_t>> newlocalParState;
    for (auto& [key, value] : parState) {
        std::vector<uint8_t> keyVector(key.begin(), key.end());
        std::size_t hashValue = hashVector(keyVector);
        int parIdx = hashValue % newParallel;
        // Otherwise, transfer it to the new master.
        dataTransfer[parToHost[parIdx]][key] = value;
    }
    // transfer data to the new master.
    for (auto& [host, data] : dataTransfer) {
        std::vector<uint8_t> dataTransferVector =
          faabric::util::serializeParState(data);
        if (host == hostIp) {
            tempParState.emplace(std::move(dataTransferVector));
            continue;
        }
        FunctionStateClient stateClient(user, function, hostToPar[host], host);
        stateClient.addPartitionState(partitionKey, dataTransferVector);
    }
    // If the parallelism Id is changed, or the this partitioned part is
    // removed.
    if (newParallelismId == -1) {
        return false;
    }
    // the redis-state will be register later.
    parallelismId = newParallelismId;
    return true;
}

void FunctionState::addTempParState(const uint8_t* buffer, size_t length)
{
    std::vector<uint8_t> tempParStateVector(buffer, buffer + length);
    tempParState.emplace(std::move(tempParStateVector));
}

bool FunctionState::combineParState()
{
    SPDLOG_TRACE("Combining partitioned state for {}/{}-{} old stateSize {}",
                 user,
                 function,
                 parallelismId,
                 stateSize);
    // calculate the new size.
    std::map<std::string, std::vector<uint8_t>> stateMap = getStateMap();
    stateMap.erase(partitionKey);
    std::map<std::string, std::vector<uint8_t>> newParStateMap;
    for (const auto& partialParVec : tempParState) {
        std::map<std::string, std::vector<uint8_t>> partialParMap =
          faabric::util::deserializeParState(partialParVec);
        for (auto& [key, value] : partialParMap) {
            newParStateMap[key] = value;
        }
    }
    stateMap[partitionKey] = faabric::util::serializeParState(newParStateMap);
    std::vector<uint8_t> newStateVec =
      faabric::util::serializeFuncState(stateMap);
    stateSize = newStateVec.size();
    // Resize and allocate the new memory.
    reSize(stateSize);
    SPDLOG_TRACE("New state size is {}", stateSize);
    doSet(newStateVec.data());
    // Clean the tempParState for next repartition
    tempParState.clear();
    return true;
}

// --------------------------------------------
// Static properties and methods
// --------------------------------------------

size_t FunctionState::getStateSizeFromRemote(const std::string& userIn,
                                             const std::string& funcIn,
                                             int parallelismIdIn,
                                             const std::string& thisIP,
                                             bool lock)
{
    std::string mainIP;
    try {
        // If this Function State is not created by other workers, return 0.
        mainIP = getFunctionStateRegistry().getMasterIPForOtherMaster(
          userIn, funcIn, parallelismIdIn, thisIP);
    } catch (FunctionStateException& ex) {
        return 0;
    }
    FunctionStateClient stateClient(userIn, funcIn, parallelismIdIn, mainIP);
    size_t stateSize = stateClient.stateSize(lock);
    return stateSize;
}

}