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
TEST_CASE_METHOD(StateFixture, "Test function state sizes",
"[functionstate]")
{
    std::string user = "stream";
    std::string function = "beta";
    int parallelism = 0;

    // Empty should be none
    size_t initialSize =
      state.getFunctionStateSize(user, function, parallelism);
    REQUIRE(initialSize == 0);

    // Write a function State
    std::vector<uint8_t> k1 = { 1, 2, 3, 4, 5 };
    std::vector<uint8_t> k2 = { 2, 3, 4, 5, 6, 7, 8 };
    std::vector<uint8_t> k3 = { 3, 4, 5, 6, 7, 8 };

    std::map<std::string, std::vector<uint8_t>> functionState =
      std::map<std::string, std::vector<uint8_t>>();
    functionState["k1"] = k1;
    functionState["k2"] = k2;
    functionState["k3"] = k3;

    std::vector<uint8_t> functionStateBytes =
      faabric::util::serializeFuncState(functionState);
    size_t stateSize = functionStateBytes.size();
    auto fs = state.getFS(user, function, parallelism, stateSize);
    fs->set(functionStateBytes.data(), stateSize);

    // Read the vector
    std::vector<uint8_t> readData(
      state.getFunctionStateSize(user, function, parallelism));
    fs->get(readData.data());
    // Compare the vector

    // Get size
    REQUIRE(state.getFunctionStateSize(user, function, parallelism) ==
            stateSize);
}

// Testing for read locally
// Testing for read size and write locally
TEST_CASE_METHOD(StateFixture, "Test function state read", "[functionstate]")
{
    std::string user = "stream";
    std::string function = "beta";
    int parallelism = 0;

    // Write a function State
    std::vector<uint8_t> k1 = { 1, 2, 3, 4, 5 };
    std::vector<uint8_t> k2 = { 2, 3, 4, 5, 6, 7, 8 };
    std::vector<uint8_t> k3 = { 3, 4, 5, 6, 7, 8 };

    std::map<std::string, std::vector<uint8_t>> functionState =
      std::map<std::string, std::vector<uint8_t>>();
    functionState["k1"] = k1;
    functionState["k2"] = k2;
    functionState["k3"] = k3;

    std::vector<uint8_t> functionStateBytes =
      faabric::util::serializeFuncState(functionState);
    size_t stateSize = functionStateBytes.size();
    auto fs = state.getFS(user, function, parallelism, stateSize);
    fs->set(functionStateBytes.data(), stateSize);

    // Read this Function State
    fs = state.getFS(user, function, parallelism, stateSize);
    std::vector<uint8_t> readData(stateSize);
    fs->get(readData.data());

    // Compare the vector
    REQUIRE(readData == functionStateBytes);
    std::map<std::string, std::vector<uint8_t>> readFunctionState =
      faabric::util::deserializeFuncState(readData);
    // Print the readFunctionState
    printf("Read Function State\n");
    for (auto const& x : readFunctionState) {
        std::cout << x.first << " : ";
        for (auto const& y : x.second) {
            std::cout << +y << " ";
        }
        std::cout << std::endl;
    }

    // Get size
    REQUIRE(state.getFunctionStateSize(user, function, parallelism) ==
            stateSize);
}

// Testing for write 'increase size' locally
TEST_CASE_METHOD(StateFixture,
                 "Test function state sizes increase write",
                 "[functionstate]")
{
    std::string user = "stream";
    std::string function = "beta";
    int parallelism = 0;

    // Write a function State
    std::vector<uint8_t> k1 = { 1, 2, 3, 4, 5 };
    std::vector<uint8_t> k2 = { 2, 3, 4, 5, 6, 7, 8 };
    std::vector<uint8_t> k3 = { 3, 4, 5, 6, 7, 8 };

    std::map<std::string, std::vector<uint8_t>> functionState =
      std::map<std::string, std::vector<uint8_t>>();
    functionState["k1"] = k1;
    functionState["k2"] = k2;
    functionState["k3"] = k3;

    std::vector<uint8_t> functionStateBytes =
      faabric::util::serializeFuncState(functionState);
    size_t stateSize = functionStateBytes.size();
    auto fs = state.getFS(user, function, parallelism, stateSize);

    fs->set(functionStateBytes.data(), stateSize);

    // Get size
    REQUIRE(state.getFunctionStateSize(user, function, parallelism) ==
            stateSize);

    // Increase size
    std::vector<uint8_t> k4;
    k4.reserve(5000);
    for (uint16_t i = 1; i <= 5000; ++i) {
        k4.push_back(3);
    }
    functionState["k4"] = k4;
    functionStateBytes = faabric::util::serializeFuncState(functionState);
    stateSize = functionStateBytes.size();
    fs = state.getFS(user, function, parallelism, stateSize);
    fs->set(functionStateBytes.data(), stateSize);
    // Get size
    REQUIRE(stateSize > faabric::util::HOST_PAGE_SIZE);
    REQUIRE(state.getFunctionStateSize(user, function, parallelism) ==
            stateSize);
}

// test for repartition state
TEST_CASE_METHOD(FunctionStateServerTestFixture,
                 "Test function state repartition (no existing before)",
                 "[functionstate]")
{
    std::string user = "stream";
    std::string function = "beta";
    int parallelism = 0;

    // Write a function State
    std::string parKey = "kp";
    std::vector<uint8_t> k1 = { 1, 2, 3, 4, 5 };
    std::vector<uint8_t> k2 = { 2, 3, 4, 5, 6, 7, 8 };
    std::map<std::string, std::vector<uint8_t>> functionParState;
    functionParState["pp1"] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    functionParState["pp2"] = { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    functionParState["pp3"] = { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    functionParState["pp4"] = { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
    functionParState["pp5"] = { 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    functionParState["pp6"] = { 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    functionParState["pp7"] = { 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    functionParState["pp8"] = { 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 };
    std::vector<uint8_t> functionParStateBytes =
      faabric::util::serializeParState(functionParState);

    std::map<std::string, std::vector<uint8_t>> functionState =
      std::map<std::string, std::vector<uint8_t>>();
    functionState["k1"] = k1;
    functionState["k2"] = k2;
    functionState[parKey] = functionParStateBytes;

    std::vector<uint8_t> functionStateBytes =
      faabric::util::serializeFuncState(functionState);
    size_t stateSize = functionStateBytes.size();
    auto fs = state.getFS(user, function, parallelism, stateSize);

    // Set the function state and partition Key
    fs->set(functionStateBytes.data(), stateSize);
    fs->setPartitionKey(parKey);

    std::shared_ptr<std::map<std::string, std::string>> newFilteredStateHost =
      std::make_shared<std::map<std::string, std::string>>();
    newFilteredStateHost->emplace("stream_beta_0", faabric::util::getSystemConfig().endpointHost);
    newFilteredStateHost->emplace("stream_beta_1", "127.0.0.1");
    std::vector<uint8_t> tmpStateHost =
      faabric::util::serializeMapBinary(*newFilteredStateHost);
    std::string newFilteredStateHostStr(tmpStateHost.begin(),
                                        tmpStateHost.end());
    // Register in Redis
    faabric::redis::Redis& redis = redis::Redis::getState();
    std::string mainKey = "main_steam_beta_1";
    std::vector<uint8_t> mainIPBytes =
      faabric::util::stringToBytes("127.0.0.1");
    redis.set(mainKey, mainIPBytes);

    // Repartition the state
    fs->rePartitionState(newFilteredStateHostStr);

    // combineParState
    fs->combineParState();
    FunctionStateClient stateClient(user, function, 1, "127.0.0.1");
    stateClient.combineParState();

    // Read the function state
    size_t stateSize1 = fs->size();
    std::vector<uint8_t> readData1(stateSize1);
    fs->get(readData1.data());
    std::map<std::string, std::vector<uint8_t>> functionState1 =
      std::map<std::string, std::vector<uint8_t>>();
    functionState1["k1"] = k1;
    functionState1["k2"] = k2;
    std::map<std::string, std::vector<uint8_t>> functionParState1;
    functionParState1["pp1"] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    functionParState1["pp3"] = { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    functionParState1["pp5"] = { 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    functionParState1["pp7"] = { 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    std::vector<uint8_t> functionParStateBytes1 =
      faabric::util::serializeParState(functionParState1);
    functionState1["k1"] = k1;
    functionState1["k2"] = k2;
    functionState1[parKey] = functionParStateBytes1;
    std::vector<uint8_t> compare1 = faabric::util::serializeFuncState(
      functionState1); // compare1 is the expected value
    
    REQUIRE(compare1 == readData1);

    size_t stateSize2 = remoteState.getFS(user, function, 1, 0)->size();
    std::vector<uint8_t> readData2(stateSize2);
    remoteState.getFS(user, function, 1, stateSize2)->get(readData2.data());

    std::map<std::string, std::vector<uint8_t>> functionState2 =
      std::map<std::string, std::vector<uint8_t>>();
    std::map<std::string, std::vector<uint8_t>> functionParState2;
    functionParState2["pp2"] = { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    functionParState2["pp4"] = { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
    functionParState2["pp6"] = { 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    functionParState2["pp8"] = { 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 };
    std::vector<uint8_t> functionParStateBytes2 =
      faabric::util::serializeParState(functionParState2);
    functionState2[parKey] = functionParStateBytes2;
    std::vector<uint8_t> compare2 = faabric::util::serializeFuncState(
      functionState2); // compare2 is the expected value
    REQUIRE(compare2 == readData2);
}

}