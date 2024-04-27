#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>

#include <map>

namespace faabric::planner {
/* This helper struct encapsulates the internal state of the planner
 */
struct PlannerState
{
    // Accounting of the hosts that are registered in the system and responsive
    // We deliberately use the host's IP as unique key, but assign a unique host
    // id for redundancy
    std::map<std::string, std::shared_ptr<Host>> hostMap;

    // Double-map holding the message results. The first key is the app id. For
    // each app id, we keep a map of the message id, and the actual message
    // result
    std::map<int, std::map<int, std::shared_ptr<faabric::Message>>> appResults;

    // Map holding the hosts that have registered interest in getting an app
    // result
    std::map<int, std::vector<std::string>> appResultWaiters;

    // Map keeping track of the requests that are in-flight
    faabric::batch_scheduler::InFlightReqs inFlightReqs;

    // Map keeping track of pre-loaded scheduling decisions that bypass the
    // planner's scheduling
    std::map<int, std::shared_ptr<batch_scheduler::SchedulingDecision>>
      preloadedSchedulingDecisions;

    // Helper coutner of the total number of migrations
    std::atomic<int> numMigrations = 0;

    // The metrics for each function-parallelismId
    // Map<User-Function-ParallelismId, Metrics>
    std::map<std::string, std::shared_ptr<FunctionMetrics>> functionMetrics;

    // The metrics for the whole chain function application (Same chain Id)
    // For chain functions, it is the metrics from the first function is invoked
    // until the last function is finished.
    // We don't record parallelism at first since the new BatchRequest can has
    // multiple [chain functions source], they belong to different parallelism.
    // source_1 -> func1_1 -> func2_2 and source_2 -> func1_2 -> func2_2
    // Map<First(User-Function), Metrics>
    std::map<std::string, std::shared_ptr<FunctionMetrics>>
      chainFunctionMetrics;

    // The count of the chain function invocation inflight
    // This is used to get the total processing time of the chain function
    // invocation
    // Map<chainedId, count>
    std::map<int, int> chainedInflights;
};
}
