#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/state/FunctionState.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/state/State.h>
#include <faabric/transport/common.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/macros.h>
#include <faabric/util/serialization.h>

#include <wait.h>

using namespace faabric::state;

namespace tests {

class SimpleFunctionStateServerTestFixture
  : public StateFixture
  , public ConfFixture
{
  public:
    SimpleFunctionStateServerTestFixture()
      : server(faabric::state::getGlobalState())
    {
        conf.stateMode = "inmemory";
        // Initialize the dataA
        std::vector<uint8_t> k1 = { 1, 2, 3, 4, 5 };
        std::vector<uint8_t> k2 = { 2, 3, 4, 5, 6, 7, 8 };
        std::vector<uint8_t> k3 = { 3, 4, 5, 6, 7, 8 };

        // std::vector<uint8_t> k1 = { 9, 8, 9, 9, 7, 7, 6, 5, 4 };
        // std::vector<uint8_t> k2 = { 3, 1, 1, 2, 3, 4, 5, 6 };
        // std::vector<uint8_t> k3 = { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 };

        std::map<std::string, std::vector<uint8_t>> tmpState =
          std::map<std::string, std::vector<uint8_t>>();
        tmpState["k1"] = k1;
        tmpState["k2"] = k2;
        tmpState["k3"] = k3;
        dataA = faabric::util::serializeFuncState(tmpState);
        // print the data A into the format of {v1, v2, v3 ...}
        // std::cout << "{";
        // for (size_t i = 0; i < dataA.size(); ++i) {
        //     std::cout << static_cast<int>(dataA[i]);
        //     if (i < dataA.size() - 1) {
        //         std::cout << ", ";
        //     }
        // }
        // std::cout << "}" << std::endl;
        // Initialize the dataB
        dataB = faabric::util::serializeFuncState(tmpState);

        server.start();
    }

    ~SimpleFunctionStateServerTestFixture() { server.stop(); }

  protected:
    StateServer server;

    const char* userA = "stream";
    const char* funcA = "funcA";
    const char* funcB = "funcB";

    uint32_t parallelismId = 0;

    std::vector<uint8_t> dataA;
    std::vector<uint8_t> dataB;
};

TEST_CASE_METHOD(SimpleFunctionStateServerTestFixture,
                 "Test function state request/ response",
                 "[functionserverstate]")
{
    std::vector<uint8_t> actual(dataA.size(), 0);

    // Prepare a key-value with data
    auto fsA = state.getFS(userA, funcA, parallelismId, dataA.size());
    fsA->set(dataA.data());

    // Prepare a key-value with no data (but a size)
    auto fsB = state.getFS(userA, funcB, parallelismId, dataA.size());

    // Prepare a key-value with same key but different data (for pushing)
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    auto fsADuplicate =
      FunctionState(userA, funcA, parallelismId, thisHost, dataB.size());
    fsADuplicate.set(dataB.data());

    FunctionStateClient client(userA, funcA, parallelismId, DEFAULT_STATE_HOST);

    SECTION("State size")
    {
        size_t actualSize = client.stateSize();
        REQUIRE(actualSize == dataA.size());
    }

    SECTION("State pull multi chunk")
    {
        // std::vector<uint8_t> part1(dataA.begin(), dataA.begin() + 10);
        // std::vector<uint8_t> part2(dataA.begin() + 10, dataA.begin() + 20);
        // std::vector<uint8_t> part3(dataA.begin() + 21, dataA.end());

        // Deliberately overlap chunks
        StateChunk chunkA(0, 10, nullptr);
        StateChunk chunkB(10, 10, nullptr);
        StateChunk chunkC(20, dataA.size() - 20, nullptr);

        std::vector<StateChunk> chunks = { chunkA, chunkB, chunkC };
        std::vector<uint8_t> expected = { 0,  0, 0, 2, 107, 49, 0,   0,  0, 5,
                                          1,  2, 3, 4, 5,   0,  0,   0,  2, 107,
                                          50, 0, 0, 0, 7,   2,  3,   4,  5, 6,
                                          7,  8, 0, 0, 0,   2,  107, 51, 0, 0,
                                          0,  6, 3, 4, 5,   6,  7,   8 };
        client.pullChunks(chunks, actual.data());
        REQUIRE(actual == expected);
    }

    SECTION("State push multi chunk")
    {
        std::vector<uint8_t> chunkDataA = { 0, 0, 0, 2, 107, 49, 0, 0, 0, 9,
                                            9, 8, 9, 9, 7,   7,  6, 5, 4, 0 };
        std::vector<uint8_t> chunkDataB = { 0, 0, 2, 107, 50, 0, 0, 0, 8, 3,
                                            1, 1, 2, 3,   4,  5, 6, 0, 0, 0 };
        std::vector<uint8_t> chunkDataC = { 2, 107, 51, 0, 0, 0, 12, 7, 7, 7,
                                            7, 7,   7,  7, 7, 7, 7,  7, 7 };

        StateChunk chunkA(0, chunkDataA);
        StateChunk chunkB(chunkDataA.size(), chunkDataB);
        StateChunk chunkC(chunkDataA.size() + chunkDataB.size(), chunkDataC);

        size_t totalSize =
          chunkDataA.size() + chunkDataB.size() + chunkDataC.size();
        std::vector<StateChunk> chunks = { chunkA, chunkB, chunkC };
        client.pushChunks(chunks, totalSize);
        actual.resize(totalSize);
        // Check expectation
        std::vector<uint8_t> expected = {
            0, 0,   0,  2,   107, 49, 0,  0, 0, 9, 9, 8, 9, 9, 7, 7, 6, 5, 4, 0,
            0, 0,   2,  107, 50,  0,  0,  0, 8, 3, 1, 1, 2, 3, 4, 5, 6, 0, 0, 0,
            2, 107, 51, 0,   0,   0,  12, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
        };
        fsA->get(actual.data());
        REQUIRE(actual == expected);
    }

    SECTION("State push multi chunk with size increase")
    {
        // Initialize vectors
        std::vector<uint8_t> chunkDataA(4000, 1);
        std::vector<uint8_t> chunkDataB(4000, 2);
        std::vector<uint8_t> chunkDataC(4000, 3);

        // Initialize chunks
        StateChunk chunkA(0, chunkDataA);
        StateChunk chunkB(4000, chunkDataB);
        StateChunk chunkC(8000, chunkDataC);
        size_t totalSize = 12000;
        std::vector<StateChunk> chunks = { chunkA, chunkB, chunkC };
        client.pushChunks(chunks, totalSize);
        actual.resize(totalSize);
        // Check expectation
        std::vector<uint8_t> expected;
        expected.insert(expected.end(), chunkDataA.begin(), chunkDataA.end());
        expected.insert(expected.end(), chunkDataB.begin(), chunkDataB.end());
        expected.insert(expected.end(), chunkDataC.begin(), chunkDataC.end());
        fsA->get(actual.data());
        // print the address of actual and expected
        std::cout << "actual: " << &actual << std::endl;
        std::cout << "expected: " << &expected << std::endl;
        REQUIRE(actual == expected);
    }
}
}
