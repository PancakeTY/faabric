#pragma once

#include <cstdint>
#include <faabric/state/FunctionStateRegistry.h>
#include <faabric/state/StateKeyValue.h>
#include <map>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::state {

class FunctionState
{
  public:
    FunctionState(const std::string& userIn,
                  const std::string& functionIn,
                  int parallelismIdIn,
                  const std::string& hostIpIn,
                  size_t stateSizeIn);

    FunctionState(const std::string& userIn,
                  const std::string& functionIn,
                  int parallelismIdIn,
                  const std::string& hostIpIn);

    static size_t getStateSizeFromRemote(const std::string& userIn,
                                         const std::string& funcIn,
                                         int parallelismIdIn,
                                         const std::string& thisIPIn);
    size_t size() const;
    void setPartitionKey(std::string key);
    void set(const uint8_t* buffer);
    void set(const uint8_t* buffer, long length);
    void reSize(long length);
    void get(uint8_t* buffer);
    // setChunk function is only used for mastser to receive updated data
    void setChunk(long offset, const uint8_t* buffer, size_t length);
    // getChunk function is only used for mastser to transfer data
    uint8_t* getChunk(long offset, long len);
    static uint32_t waitOnRedisRemoteLockFunc(const std::string& redisKey);
    static void clearAll(bool global);
    // After receiving the repartition request from planner, it send its
    // partition state to other state server.
    // return false means this function state is no longer used.
    bool rePartitionState(const std::string& newStateHost);
    void addTempParState(const uint8_t* buffer, size_t length);
    bool combineParState();
    const std::string user;
    const std::string function;
    // the default parallelism ID is 0
    int parallelismId;
    bool isMaster = false;

  private:
    std::shared_mutex stateMutex;
    size_t stateSize;
    FunctionStateRegistry& stateRegistry;
    // The host IP is the local IP
    const std::string hostIp;
    // The master IP is the IP of the master node
    const std::string masterIp;
    // the key of keyValue with is partition state which is not partition input.
    std::string partitionKey;
    std::atomic<bool> fullyAllocated = false;
    size_t sharedMemSize = 0;
    void* sharedMemory = nullptr;
    std::unordered_map<std::string, std::vector<uint8_t>> state;
    // Configure the Size of State by using Chunks.
    void checkSizeConfigured();
    void zeroDirtyMask();
    void configureSize();
    void reserveStorage();
    void allocateChunk(long offset, size_t length);
    std::vector<StateChunk> getAllChunks();

    void doSet(const uint8_t* data);
    void pushToRemote();
    void doPull();
    void pullFromRemote();
    std::map<std::string, std::vector<uint8_t>> getStateMap();
    // It parsed the partition state from the sharedmemory space
    std::map<std::string, std::vector<uint8_t>> getParStateMap();
    // It added the partition state to the temp state. After the instruction
    // from the planner, it will be added to the state.
    std::set<std::vector<uint8_t>> tempParState;
};

class FunctionStateException : public std::runtime_error
{
  public:
    explicit FunctionStateException(const std::string& message)
      : runtime_error(message)
    {}
};
}