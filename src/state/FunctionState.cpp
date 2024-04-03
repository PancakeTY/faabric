#include <faabric/state/FunctionState.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
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

    // Unique lock for setting the whole value
    FullLock lock(stateMutex);
    doSet(buffer);
}

void FunctionState::reSize(long length)
{
    checkSizeConfigured();
    stateSize = length;
    // If new length is bigger than the reserved size, reallocate it.
    FullLock lock(stateMutex);
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

void FunctionState::set(const uint8_t* buffer, long length)
{
    reSize(length);
    doSet(buffer);
    if (!isMaster) {
        pushToRemote();
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

void FunctionState::pushToRemote()
{
    if (isMaster) {
        return;
    }
    std::vector<StateChunk> allChunks = getAllChunks();
    FunctionStateClient cli(user, function, parallelismId, masterIp);
    cli.pushChunks(allChunks, stateSize);
}

// Only the Master node can return its data, otherwise pull at first.
void FunctionState::get(uint8_t* buffer)
{
    doPull();

    SharedLock lock(stateMutex);
    auto bytePtr = BYTES(sharedMemory);
    std::copy(bytePtr, bytePtr + stateSize, buffer);
}

// In our function, doPull is called to retrive the data from the master node.
void FunctionState::doPull()
{
    if (isMaster) {
        return;
    }
    checkSizeConfigured();
    // Unique lock on the whole value
    faabric::util::FullLock lock(stateMutex);
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

void FunctionState::setChunk(long offset, const uint8_t* buffer, size_t length)
{
    checkSizeConfigured();

    FullLock lock(stateMutex);

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

// --------------------------------------------
// Static properties and methods
// --------------------------------------------

size_t FunctionState::getStateSizeFromRemote(const std::string& userIn,
                                             const std::string& funcIn,
                                             int parallelismIdIn,
                                             const std::string& thisIP)
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
    size_t stateSize = stateClient.stateSize();
    return stateSize;
}

}