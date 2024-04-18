#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/redis/Redis.h>
#include <faabric/state/FunctionState.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/state/StateServer.h>
#include <faabric/util/config.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/network.h>
#include <faabric/util/serialization.h>
#include <faabric/util/state.h>

#include <sys/mman.h>

using namespace faabric::state;

namespace tests {

class FunctionStateServerTestFixture
  : public StateFixture
  , public ConfFixture
{
  public:
    // Set up a local server with a *different* state instance to the main
    // thread. This way we can fake the main/ non-main setup
    FunctionStateServerTestFixture()
      : remoteState(LOCALHOST)
      , stateServer(remoteState)
    {
        // Start the state server
        SPDLOG_DEBUG("Running state server on {}", LOCALHOST);
        stateServer.start();
    }

    ~FunctionStateServerTestFixture() { stateServer.stop(); }

  protected:
    state::State remoteState;
    state::StateServer stateServer;

  private:
};

// Testing for read size and write locally
TEST_CASE_METHOD(FunctionStateServerTestFixture,
                 "Test function state sizes with lock",
                 "[functionstatelock]")
{
    std::string user = "stream";
    std::string function = "beta";
    int parallelism = 0;

    // Register in Redis
    faabric::redis::Redis& redis = redis::Redis::getState();
    std::string mainKey = "main_stream_beta_0";
    std::vector<uint8_t> mainIPBytes = faabric::util::stringToBytes(
      faabric::util::getSystemConfig().endpointHost);
    redis.set(mainKey, mainIPBytes);
    // Create the function
    {
        auto fs = state.createFS(user, function, parallelism, "");
        if (!fs->isMaster) {
            throw std::runtime_error(
              "StateServer receive create request, but created state "
              "is not master");
        }
    }

    // Empty should be none
    size_t initialSize =
      state.getFunctionStateSize(user, function, parallelism, true);
    REQUIRE(initialSize == 0);

    // Write a function State
    std::map<std::string, std::vector<uint8_t>> inputState;
    inputState["k1"] = { 1, 2, 3, 4, 5 };
    inputState["k2"] = { 2, 3, 4, 5, 6, 7, 8 };

    std::vector<uint8_t> inputBytes =
      faabric::util::serializeFuncState(inputState);
    size_t stateSize = inputBytes.size();

    auto fs = state.getFS(user, function, parallelism, stateSize);
    fs->set(inputBytes.data(), stateSize, true);

    // Get size
    size_t newSize =
      state.getFunctionStateSize(user, function, parallelism, true);
    std::vector<uint8_t> newBytes(newSize);
    fs->get(newBytes.data());
    fs->unlockMasterWrite();
    // Get size
    REQUIRE(newSize == stateSize);
    REQUIRE(newBytes == inputBytes);
}

// Testing for read size and write remotely
TEST_CASE_METHOD(FunctionStateServerTestFixture,
                 "Test function state sizes remotely with lock",
                 "[functionstatelock]")
{
    std::string user = "stream";
    std::string function = "beta";
    int parallelism = 0;

    // Register in Redis
    faabric::redis::Redis& redis = redis::Redis::getState();
    std::string mainKey = "main_stream_beta_0";
    std::vector<uint8_t> mainIPBytes = faabric::util::stringToBytes(LOCALHOST);
    redis.set(mainKey, mainIPBytes);
    // Create the function
    {
        auto fs = remoteState.createFS(user, function, parallelism, "");
        if (!fs->isMaster) {
            throw std::runtime_error(
              "StateServer receive create request, but created state "
              "is not master");
        }
    }

    // Get the iniliazed state size and lock the state.
    size_t initialSize =
      state.getFunctionStateSize(user, function, parallelism, true);
    REQUIRE(initialSize == 0);

    // Write a function State
    std::map<std::string, std::vector<uint8_t>> inputState;
    inputState["k1"] = { 1, 2, 3, 4, 5 };
    inputState["k2"] = { 2, 3, 4, 5, 6, 7, 8 };

    std::vector<uint8_t> inputBytes =
      faabric::util::serializeFuncState(inputState);
    size_t stateSize = inputBytes.size();

    // Write the initialize data back and unlock
    auto fs = state.getFS(user, function, parallelism, stateSize);
    fs->set(inputBytes.data(), stateSize, true);

    // Get size
    size_t newSize =
      state.getFunctionStateSize(user, function, parallelism, true);
    std::vector<uint8_t> newBytes(newSize);
    fs->get(newBytes.data());
    fs->unlockMasterWrite();
    // Get size
    REQUIRE(newSize == stateSize);
    REQUIRE(newBytes == inputBytes);

    auto remoteFS = remoteState.getFS(user, function, parallelism);
    size_t remoteSize = remoteFS->size();
    std::vector<uint8_t> remoteBytes(remoteSize);
    remoteFS->get(remoteBytes.data());
    REQUIRE(remoteSize == stateSize);
    REQUIRE(remoteBytes == inputBytes);
}

TEST_CASE_METHOD(FunctionStateServerTestFixture,
                 "Test function partitioned state sizes remotely with lock",
                 "[testnow]")
{
    std::string user = "stream";
    std::string function = "beta";
    std::string partitionKey = "kp";
    int parallelism = 0;

    // Register in Redis
    faabric::redis::Redis& redis = redis::Redis::getState();
    std::string mainKey = "main_stream_beta_0";
    std::vector<uint8_t> mainIPBytes =
    faabric::util::stringToBytes(LOCALHOST); redis.set(mainKey, mainIPBytes);
    // Create the function
    {
        auto fs =
          remoteState.createFS(user, function, parallelism, partitionKey);
        if (!fs->isMaster) {
            throw std::runtime_error(
              "StateServer receive create request, but created state "
              "is not master");
        }
    }

    // Read size and lock the data
    SPDLOG_DEBUG("Get the iniliazed state size and lock the state.");
    size_t initialSize =
      state.getFunctionStateSize(user, function, parallelism, true);
    REQUIRE(initialSize == 0);

    // Write a function State
    std::map<std::string, std::vector<uint8_t>> inputState;
    inputState["k1"] = { 1, 2, 3, 4, 5 };
    inputState["k2"] = { 2, 3, 4, 5, 6, 7, 8 };
    std::map<std::string, std::vector<uint8_t>> partitionState;
    partitionState["p1"] = { 1, 2, 3, 4, 5 };
    partitionState["p2"] = { 2, 3, 4, 5, 6, 7, 8 };
    inputState[partitionKey] =
    faabric::util::serializeParState(partitionState);

    std::vector<uint8_t> inputBytes =
      faabric::util::serializeFuncState(inputState);
    size_t stateSize = inputBytes.size();

    // Write data back and unlock it
    SPDLOG_DEBUG("Write data back and unlock state.");
    auto fs = state.getFS(user, function, parallelism, stateSize);
    fs->set(inputBytes.data(), stateSize, true);

    // Get size
    SPDLOG_DEBUG("Get the size of the state and lock it again.");
    fs->unlockMasterWrite();
    size_t newSize =
      state.getFunctionStateSize(user, function, parallelism, true);
    fs = state.getFS(user, function, parallelism, newSize);
    uint8_t* newData = fs->get();

    SPDLOG_DEBUG("Get the new data now.");

    // Get size
    REQUIRE(newSize == stateSize);
    std::vector<uint8_t> newBytes =
      std::vector<uint8_t>(newData, newData + stateSize);
    REQUIRE(newBytes == inputBytes);

    auto remoteFS = remoteState.getFS(user, function, parallelism);
    size_t remoteSize = remoteFS->size();
    uint8_t* remoteData = remoteFS->get();
    std::vector<uint8_t> remoteBytes =
      std::vector<uint8_t>(remoteData, remoteData + remoteSize);
    REQUIRE(remoteSize == stateSize);
    REQUIRE(remoteBytes == inputBytes);
}

// Testing for lock and unlock locally
TEST_CASE_METHOD(FunctionStateServerTestFixture,
                 "Test function lock unlock locally",
                 "[testnowxxx]")
{
    std::string user = "stream";
    std::string function = "beta";
    std::string partitionKey = "kp";
    int parallelism = 0;

    // Register in Redis
    faabric::redis::Redis& redis = redis::Redis::getState();
    std::string mainKey = "main_stream_beta_0";
    std::vector<uint8_t> mainIPBytes =
    faabric::util::stringToBytes(LOCALHOST); redis.set(mainKey, mainIPBytes);
    // Create the function
    {
        auto fs =
          remoteState.createFS(user, function, parallelism, partitionKey);
        if (!fs->isMaster) {
            throw std::runtime_error(
              "StateServer receive create request, but created state "
              "is not master");
        }
    }

    // Get the iniliazed state size and lock the state.
    size_t initialSize =
      state.getFunctionStateSize(user, function, parallelism, true);
    REQUIRE(initialSize == 0);

    // Write a function State
    std::map<std::string, std::vector<uint8_t>> inputState;
    inputState["k1"] = { 1, 2, 3, 4, 5 };
    std::map<std::string, std::vector<uint8_t>> partitionState;
    partitionState["p1"] = { 1, 2, 3, 4, 5 };
    partitionState["p2"] = { 2, 3, 4, 5, 6, 7, 8 };
    inputState[partitionKey] =
    faabric::util::serializeParState(partitionState);

    std::vector<uint8_t> inputBytes =
      faabric::util::serializeFuncState(inputState);
    size_t inputSize = inputBytes.size();

    // Write and unlock.
    auto fs = state.getFS(user, function, parallelism, inputSize);
    fs->set(inputBytes.data(), inputSize, true);

    // Create two thread, read and lock the fucntion state at the same time.
    // Function to simulate state access that requires locking
    auto accessFunctionState = [&](int id) {
        SPDLOG_DEBUG("Thread {} starting", id);
        // Read and Lock the Data
        int32_t stateSize =
          state.getFunctionStateSize(user, function, parallelism, true);
        std::vector<uint8_t> bytes(stateSize);
        auto fs1 = state.getFS(user, function, parallelism, stateSize);
        SPDLOG_DEBUG("Thread {} got state size for {}/{}-{} : {}",
                     id,
                     user,
                     function,
                     parallelism,
                     stateSize);
        // Read the Data
        fs->get(bytes.data());
        SPDLOG_DEBUG("Thread {} got state data", id);
        std::map<std::string, std::vector<uint8_t>> tmpState =
          faabric::util::deserializeFuncState(bytes);
        std::map<std::string, std::vector<uint8_t>> tmpPartitionState =
          faabric::util::deserializeParState(tmpState[partitionKey]);
        // Change the data
        tmpPartitionState["p1"][0]++;
        tmpState[partitionKey] =
          faabric::util::serializeParState(tmpPartitionState);
        std::vector<uint8_t> tmpBytes =
          faabric::util::serializeFuncState(tmpState);
        // Sleep for 3 seconds.
        std::this_thread::sleep_for(std::chrono::seconds(3));
        size_t tmpSize = tmpBytes.size();
        SPDLOG_DEBUG("Thread {} going to write state data", id);
        auto fs2 = state.getFS(user, function, parallelism, tmpSize);
        // Write data back and unlock
        fs2->set(tmpBytes.data(), tmpSize, true);
        SPDLOG_DEBUG("Thread {} finish writing state data", id);
        return size_t(0);
    };

    // Create two threads
    // Start threads
    std::thread Thread1(accessFunctionState, 1);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::thread Thread2(accessFunctionState, 2);

    // Join threads
    Thread1.join();
    Thread2.join();

    // Additionally, verify that data read is as expected
    auto finalFS = state.getFS(user, function, parallelism);
    size_t finalSize = finalFS->size();
    uint8_t* finalData = finalFS->get();
    std::vector<uint8_t> finalBytes =
      std::vector<uint8_t>(finalData, finalData + finalSize);

    std::map<std::string, std::vector<uint8_t>> expState;
    expState["k1"] = { 1, 2, 3, 4, 5 };
    std::map<std::string, std::vector<uint8_t>> expPartitionState;
    expPartitionState["p1"] = { 3, 2, 3, 4, 5 };
    expPartitionState["p2"] = { 2, 3, 4, 5, 6, 7, 8 };
    expState[partitionKey] =
      faabric::util::serializeParState(expPartitionState);
    std::vector<uint8_t> expBytes =
    faabric::util::serializeFuncState(expState); size_t expStateSize =
    expBytes.size();

    REQUIRE(finalSize == expStateSize);
    REQUIRE(finalBytes == expBytes);
}
}