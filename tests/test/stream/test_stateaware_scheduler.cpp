#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/PlannerServer.h>
#include <faabric/util/serialization.h>

#include <memory>
using namespace faabric::batch_scheduler;

namespace tests {

class SchedulerPlannerClientFixture : public BatchSchedulerFixture
{
  public:
    SchedulerPlannerClientFixture()
      : BatchSchedulerFixture()
      , conf(initializeConfiguration())
      , plannerCli(faabric::planner::getPlannerClient())
      , plannerServer()
    {
        batchScheduler = getBatchScheduler();
        stateAwareScheduler = std::dynamic_pointer_cast<
          faabric::batch_scheduler::StateAwareScheduler>(batchScheduler);
        plannerServer.start();
    }

    std::string localHost = faabric::util::getSystemConfig().endpointHost;

    void registerStateFunction(const std::string& user,
                               const std::string& func,
                               const char* inputKey,
                               const char* stateKey,
                               std::string host)
    {
        // Register the function state to the planner
        auto regReq = std::make_shared<faabric::FunctionStateRegister>();
        regReq->set_user(user);
        regReq->set_func(func);
        regReq->set_host(host);
        regReq->set_isparition(false);
        if (inputKey != nullptr && stateKey != nullptr) {
            regReq->set_isparition(true);
            regReq->set_pinputkey(inputKey);
            regReq->set_pstatekey(stateKey);
        }
        auto& plannerCli = faabric::planner::getPlannerClient();
        plannerCli.registerFunctionState(regReq);
    }

  protected:
    faabric::util::SystemConfig& conf;
    faabric::planner::PlannerClient& plannerCli;
    faabric::planner::PlannerServer plannerServer;
    std::shared_ptr<faabric::batch_scheduler::StateAwareScheduler>
      stateAwareScheduler;

  private:
    static faabric::util::SystemConfig& initializeConfiguration()
    {
        auto& cfg = faabric::util::getSystemConfig();
        cfg.plannerHost = LOCALHOST;
        cfg.logLevel = "trace";
        cfg.batchSchedulerMode = "state-aware";
        // Additional configuration setup
        return cfg;
    }
};

// For the non-state-funciton, it should acts as the binpack!
TEST_CASE_METHOD(SchedulerPlannerClientFixture,
                 "Test scheduling of new stateful requests with StateAware",
                 "[stateaware-scheduler]")
{
    // Register the position of function1
    registerStateFunction("stream", "function1", nullptr, nullptr, localHost);
    // To mock new requests (i.e. DecisionType::NEW), we always set the
    // InFlightReqs map to an empty map
    BatchSchedulerConfig config = {
        .hostMap = {},
        .inFlightReqs = {},
        .expectedDecision = SchedulingDecision(appId, groupId),
    };

    SECTION("StateAware scheduler gives up if not enough slots are available")
    {
        config.hostMap =
          buildHostMap({ "another", localHost }, { 2, 2 }, { 1, 1 });
        ber = faabric::util::batchExecFactory("stream", "function1", 3);
        config.expectedDecision = NOT_ENOUGH_SLOTS_DECISION;
    }

    SECTION("Scheduling to state-host if available, otherwise binpack")
    {
        config.hostMap = buildHostMap({ "foo", localHost }, { 5, 3 }, { 0, 0 });
        ber = faabric::util::batchExecFactory("stream", "function1", 5);
        config.expectedDecision = buildExpectedDecision(
          ber, { localHost, localHost, localHost, "foo", "foo" });
    }

    // Register the position of partitioned stateful function1
    registerStateFunction(
      "stream", "function2", "inputKey", "keyMap", localHost);

    SECTION("Scheduling to state-host if available, otherwise binpack")
    {
        config.hostMap = buildHostMap({ "foo", localHost }, { 5, 3 }, { 0, 0 });
        ber = faabric::util::batchExecFactory("stream", "function2", 5);
        config.expectedDecision = buildExpectedDecision(
          ber, { localHost, localHost, localHost, "foo", "foo" });
    }

    actualDecision = *batchScheduler->makeSchedulingDecision(
      config.hostMap, config.inFlightReqs, ber);
    compareSchedulingDecisions(actualDecision, config.expectedDecision);
}

// For the non-state-funciton, it should acts as the binpack!
TEST_CASE_METHOD(SchedulerPlannerClientFixture,
                 "Test scheduling of new stateful requests with StateAware "
                 "with multiple parallelism",
                 "[stateaware-scheduler]")
{
    // To mock new requests (i.e. DecisionType::NEW), we always set the
    // InFlightReqs map to an empty map
    BatchSchedulerConfig config = {
        .hostMap = {},
        .inFlightReqs = {},
        .expectedDecision = SchedulingDecision(appId, groupId),
    };

    SECTION("Changing the parallelism of the function")
    {
        // Register the position of function1
        registerStateFunction(
          "stream", "function1", nullptr, nullptr, "localHost1");
        config.hostMap = buildHostMap({ "localHost1",
                                        "localHost2",
                                        "localHost5",
                                        "localHost3",
                                        "localHost4",
                                        "localHost6" },
                                      { 2, 2, 5, 2, 5, 6 },
                                      { 0, 0, 0, 0, 0, 0 });
        stateAwareScheduler->increaseFunctionParallelism("stream_function1",
                                                         config.hostMap);
        stateAwareScheduler->increaseFunctionParallelism("stream_function1",
                                                         config.hostMap);
        // check the registeration in Redis
        redis::Redis& redis = redis::Redis::getState();
        std::vector<uint8_t> mainIPBytes1 =
          redis.get("main_stream_function1_1");
        std::vector<uint8_t> mainIPBytes2 =
          redis.get("main_stream_function1_2");
        std::string mainIP1(mainIPBytes1.begin(), mainIPBytes1.end());
        std::string mainIP2(mainIPBytes2.begin(), mainIPBytes2.end());
        REQUIRE(mainIP1 == "localHost2");
        REQUIRE(mainIP2 == "localHost3");
        ber = faabric::util::batchExecFactory("stream", "function1", 7);
        config.expectedDecision = buildExpectedDecision(ber,
                                                        { "localHost1",
                                                          "localHost2",
                                                          "localHost3",
                                                          "localHost1",
                                                          "localHost2",
                                                          "localHost3",
                                                          "localHost6" });
    }

    SECTION("Changing the parallelism of the partitioned stateful function")
    {
        // Register the position of function1
        registerStateFunction(
          "stream", "function2", "inputKey", "keyMap", "localHost1");
        config.hostMap = buildHostMap({ "localHost1",
                                        "localHost2",
                                        "localHost5",
                                        "localHost3",
                                        "localHost4",
                                        "localHost6" },
                                      { 2, 2, 5, 2, 5, 6 },
                                      { 0, 0, 0, 0, 0, 0 });
        stateAwareScheduler->increaseFunctionParallelism("stream_function2",
                                                         config.hostMap);
        stateAwareScheduler->increaseFunctionParallelism("stream_function2",
                                                         config.hostMap);
        // check the registeration in Redis
        redis::Redis& redis = redis::Redis::getState();
        std::vector<uint8_t> mainIPBytes1 =
          redis.get("main_stream_function2_1");
        std::vector<uint8_t> mainIPBytes2 =
          redis.get("main_stream_function2_2");
        std::string mainIP1(mainIPBytes1.begin(), mainIPBytes1.end());
        std::string mainIP2(mainIPBytes2.begin(), mainIPBytes2.end());
        REQUIRE(mainIP1 == "localHost2");
        REQUIRE(mainIP2 == "localHost3");
        // Hash of 'cat' % 3: 0
        // Hash of 'dog' % 3: 1
        // Hash of 'tiger' % 3: 1
        // Hash of 'lion' % 3: 2
        // Hash of 'rabbit' % 3: 0
        // Hash of 'puma' % 3: 0
        // Hash of 'donaldduck' % 3: 1
        // Hash of 'pig' % 3: 1
        // Hash of 'donkey' % 3: 0
        // Hash of 'mouse' % 3: 2
        // List of strings to be hashed
        ber = faabric::util::batchExecFactory("stream", "function2", 7);
        // 2, 1, 2, 0, 0, 0, 1
        std::vector<std::string> keys = { "lion",   "pig", "mouse", "donkey",
                                          "rabbit", "cat", "dog" };
        std::vector<uint8_t> value = { 1, 2, 3, 4, 5, 6, 7, 8, 8, 12, 3, 4 };
        for (int i = 0; i < keys.size(); i++) {
            std::map<std::string, std::vector<uint8_t>> keyMap;
            // Convert the string key to std::vector<uint8_t>
            std::vector<uint8_t> keyAsVector(keys[i].begin(), keys[i].end());
            keyMap["inputKey"] = keyAsVector;
            keyMap["value"] = value;
            std::vector<uint8_t> parsedInput =
              faabric::util::serializeParState(keyMap);
            ber->mutable_messages()->at(i).set_inputdata(
              std::string(parsedInput.begin(), parsedInput.end()));
        }
        // the order of decision is matching the order of request.
        config.expectedDecision = buildExpectedDecision(ber,
                                                        { "localHost3",
                                                          "localHost2",
                                                          "localHost3",
                                                          "localHost1",
                                                          "localHost1",
                                                          "localHost6",
                                                          "localHost2" });
    }
    actualDecision = *stateAwareScheduler->makeSchedulingDecision(
      config.hostMap, config.inFlightReqs, ber);
    compareSchedulingDecisions(actualDecision, config.expectedDecision);
}
}