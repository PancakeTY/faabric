#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>
#include <map>
#include <string>

namespace faabric::batch_scheduler {

class StateAwareScheduler final : public BatchScheduler
{
  public:
    std::shared_ptr<SchedulingDecision> makeSchedulingDecision(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;
    // put here (public) only for tests.
    std::shared_ptr<std::map<std::string, std::string>>
    increaseFunctionParallelism(const std::string& userFunction,
                                HostMap& hostMap);
    bool repartitionParitionedState(
      std::string userFunction,
      std::shared_ptr<std::map<std::string, std::string>> oldStateHost);

  private:
    bool isFirstDecisionBetter(
      std::shared_ptr<SchedulingDecision> decisionA,
      std::shared_ptr<SchedulingDecision> decisionB) override;

    std::vector<Host> getSortedHosts(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) override;

    /***
     * The following maps are used to store the state of the functions.
     */
    // FuctionUser : Parallelism
    std::map<std::string, int> functionParallelism;
    // FuctionUser : Counter. It is used for shuffle grouping.
    std::map<std::string, int> functionCounter;
    // FuctionUser : Host
    std::map<std::string, std::string> stateHost;
    // FuctionUser : Input Parition Key
    std::map<std::string, std::string> statePartitionBy;

    /***
     * example should be like:
     * Function Parallelism Map:
     * Function: demo_function1, Parallelism: 1
     * Function: demo_function2, Parallelism: 1

     * State Host Map:
     * Function: demo_function1_0, Host: 172.31.0.6
     * Function: demo_function2_0, Host: 172.31.0.6

     * State Partition By Map:
     * Function: demo_function1, Partitioned By: k1
     * Function: demo_function2, Partitioned By: k2
    ***/
    bool registerState(const std::string& userFunction,
                       const std::string& host,
                       const std::string& partitionBy = "") override;
    // return the parallelism index of this function, which is align with state.
    int getParallelismIndex(const std::string& userFunction,
                            const faabric::Message& msg);
};
}