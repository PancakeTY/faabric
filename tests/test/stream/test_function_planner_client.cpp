#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

using namespace faabric::planner;

namespace tests {


// state aware scheduler
class PlannerClientServerConfFixture : public ConfFixture
{
  public:
    PlannerClientServerConfFixture()
      : conf(initializeConfiguration())
      , plannerCli(faabric::planner::getPlannerClient())
      , plannerServer()
    {
        plannerServer.start();
        plannerCli.ping();
    }

    ~PlannerClientServerConfFixture()
    {
        plannerServer.stop();
        faabric::planner::getPlanner().reset();
    }

  protected:
    faabric::util::SystemConfig& conf;
    faabric::planner::PlannerClient& plannerCli;
    faabric::planner::PlannerServer plannerServer;

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

TEST_CASE_METHOD(PlannerClientServerConfFixture,
                 "Test registering function without partitionBy",
                 "[functionplanner]")
{
    REQUIRE(conf.plannerHost == "127.0.0.1");

    // Register the function state to the planner
    auto regReq = std::make_shared<faabric::FunctionStateRegister>();
    std::string localHost = faabric::util::getSystemConfig().endpointHost;
    regReq->set_user("demo");
    regReq->set_func("function1");
    regReq->set_host(localHost);
    regReq->set_isparition(false);
    REQUIRE_NOTHROW(plannerCli.registerFunctionState(regReq));
    // It is allowed for duplicated registration
    REQUIRE_NOTHROW(plannerCli.registerFunctionState(regReq));
}

TEST_CASE_METHOD(PlannerClientServerConfFixture,
                 "Test registering function with partitionBy",
                 "[functionplanner]")
{
    std::string localHost = faabric::util::getSystemConfig().endpointHost;
    auto regReq = std::make_shared<faabric::FunctionStateRegister>();
    regReq->set_user("demo");
    regReq->set_func("function1");
    regReq->set_host(localHost);
    regReq->set_isparition(false);
    REQUIRE_NOTHROW(plannerCli.registerFunctionState(regReq));
    // Registe it again with partitionBy.
    regReq->set_user("demo");
    regReq->set_func("function1");
    regReq->set_host(localHost);
    regReq->set_isparition(true);
    regReq->set_pinputkey("k1");
    regReq->set_pstatekey("s1");
    REQUIRE_NOTHROW(plannerCli.registerFunctionState(regReq));
    // Registe paritioned state directly.
    regReq->set_user("demo");
    regReq->set_func("function2");
    regReq->set_host(localHost);
    regReq->set_isparition(true);
    regReq->set_pinputkey("k2");
    regReq->set_pstatekey("s2");
    REQUIRE_NOTHROW(plannerCli.registerFunctionState(regReq));
    // It is allowed for duplicated registration
    REQUIRE_NOTHROW(plannerCli.registerFunctionState(regReq));
}
}