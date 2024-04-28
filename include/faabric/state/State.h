#pragma once

#include <faabric/state/FunctionState.h>
#include <faabric/state/StateKeyValue.h>

#include <shared_mutex>
#include <string>

namespace faabric::state {

// State client-server API
enum StateCalls
{
    NoStateCall = 0,
    Pull = 1,
    Push = 2,
    Size = 3,
    Append = 4,
    ClearAppended = 5,
    PullAppended = 6,
    Delete = 7,
    FunctionSize = 8,
    FunctionPull = 9,
    FunctionPush = 10,
    FunctionRepartition = 11,
    // Add the new partitioned function state data
    FunctionParAdd = 12,
    FunctionParCombine = 13,
    FunctionLock = 14,
    FunctionUnlock = 15,
    FunctionCreate = 16,
    FunctionLatency = 17,
};

class State
{
  public:
    explicit State(std::string thisIPIn);

    size_t getStateSize(const std::string& user, const std::string& keyIn);

    std::shared_ptr<StateKeyValue> getKV(const std::string& user,
                                         const std::string& key,
                                         size_t size);

    std::shared_ptr<StateKeyValue> getKV(const std::string& user,
                                         const std::string& key);

    void forceClearAll(bool global);

    void deleteKV(const std::string& userIn, const std::string& keyIn);

    void deleteKVLocally(const std::string& userIn, const std::string& keyIn);

    size_t getKVCount();

    std::string getThisIP();

    // The folowing function is designed for Function State
    size_t getFunctionStateSize(const std::string& user,
                                const std::string& func,
                                int32_t parallelismId,
                                bool lock = false);

    std::shared_ptr<FunctionState> createFS(
      const std::string& user,
      const std::string& func,
      int32_t parallelismId,
      const std::string& parStateKey = "");

    std::shared_ptr<FunctionState> getFS(const std::string& user,
                                         const std::string& func,
                                         int32_t parallelismId,
                                         size_t size);

    std::shared_ptr<FunctionState> getFS(const std::string& user,
                                         const std::string& func,
                                         int32_t parallelismId);

    // In this function, it will return nullptr if no FunctionState is found.
    std::shared_ptr<FunctionState> getOnlyFS(const std::string& user,
                                             const std::string& func,
                                             int32_t parallelismId);

    void deleteFS(const std::string& user,
                  const std::string& func,
                  int32_t parallelismId);

    // Get all the metrics of the master function states from this host.
    std::map<std::string, std::map<std::string, int>> getFSMetrics();

  private:
    const std::string thisIP;

    std::unordered_map<std::string, std::shared_ptr<StateKeyValue>> kvMap;
    std::unordered_map<std::string, std::shared_ptr<FunctionState>> fsMap;

    std::shared_mutex mapMutex;
    std::shared_mutex fsmapMutex;

    std::shared_ptr<StateKeyValue> doGetKV(const std::string& user,
                                           const std::string& key,
                                           bool sizeless,
                                           size_t size);

    std::shared_ptr<FunctionState> doGetFS(const std::string& user,
                                           const std::string& func,
                                           int32_t parallelismId,
                                           bool sizeless,
                                           size_t size);
};

State& getGlobalState();
}
