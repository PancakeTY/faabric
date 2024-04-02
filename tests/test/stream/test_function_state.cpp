#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/redis/Redis.h>
#include <faabric/state/FunctionState.h>
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

class StateServerTestFixture
  : public StateFixture
  , public ConfFixture
{
  public:
    // Set up a local server with a *different* state instance to the main
    // thread. This way we can fake the main/ non-main setup
    StateServerTestFixture()
      : remoteState(LOCALHOST)
      , stateServer(remoteState)
    {
        // Start the state server
        SPDLOG_DEBUG("Running state server");
        stateServer.start();
    }

    ~StateServerTestFixture() { stateServer.stop(); }

    void setDummyData(std::vector<uint8_t> data)
    {
        dummyData = data;

        // Master the dummy data in the "remote" state
        if (!dummyData.empty()) {
            std::string originalHost = conf.endpointHost;
            conf.endpointHost = LOCALHOST;

            const std::shared_ptr<state::StateKeyValue>& kv =
              remoteState.getKV(dummyUser, dummyKey, dummyData.size());

            std::shared_ptr<InMemoryStateKeyValue> inMemKv =
              std::static_pointer_cast<InMemoryStateKeyValue>(kv);

            // Check this kv "thinks" it's main
            if (!inMemKv->isMaster()) {
                SPDLOG_ERROR("Dummy state server not main for data");
                throw std::runtime_error("Dummy state server failed");
            }

            // Set the data
            kv->set(dummyData.data());
            SPDLOG_DEBUG(
              "Finished setting main for test {}/{}", kv->user, kv->key);

            conf.endpointHost = originalHost;
        }
    }

  protected:
    std::string dummyUser;
    std::string dummyKey;

    state::State remoteState;
    state::StateServer stateServer;

  private:
    std::vector<uint8_t> dummyData;
};

// Testing for read size and write locally
TEST_CASE_METHOD(StateFixture, "Test function state sizes", "[functionstate]")
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
}
