#pragma once

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/planner/PlannerState.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/queue.h>

#include <shared_mutex>

namespace faabric::planner {
enum FlushType
{
    NoFlushType = 0,
    Hosts = 1,
    Executors = 2,
    SchedulingState = 3,
};

/* The planner is a standalone component that has a global view of the state
 * of a distributed faabric deployment.
 */
class Planner
{
  public:
    Planner();

    ~Planner();

    // ----------
    // Planner config
    // ----------

    PlannerConfig getConfig();

    void printConfig() const;

    // ----------
    // Util public API
    // ----------

    bool reset();

    bool flush(faabric::planner::FlushType flushType);

    // ----------
    // Host membership public API
    // ----------

    std::vector<std::shared_ptr<Host>> getAvailableHosts();

    bool registerHost(const Host& hostIn, bool overwrite);

    // Best effort host removal. Don't fail if we can't
    void removeHost(const Host& hostIn);

    // ----------
    // Request scheduling public API
    // ----------

    // Setters/getters for individual message results

    void setMessageResult(std::shared_ptr<faabric::Message> msg,
                          bool locked = false);

    void setMessageResultWitoutLock(std::shared_ptr<faabric::Message> msg);

    void setMessageResultBatch(
      std::shared_ptr<faabric::BatchExecuteRequest> batchMsg);

    std::shared_ptr<faabric::Message> getMessageResult(
      std::shared_ptr<faabric::Message> msg);

    // Setter/Getter to bypass the planner's scheduling for a specific app
    void preloadSchedulingDecision(
      int appId,
      std::shared_ptr<batch_scheduler::SchedulingDecision> decision);

    std::shared_ptr<batch_scheduler::SchedulingDecision>
    getPreloadedSchedulingDecision(int32_t appId,
                                   std::shared_ptr<BatchExecuteRequest> ber);

    // Get all the results recorded for one batch
    std::shared_ptr<faabric::BatchExecuteRequestStatus> getBatchResults(
      int32_t appId);

    std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
    getSchedulingDecision(std::shared_ptr<BatchExecuteRequest> req);

    faabric::batch_scheduler::InFlightReqs getInFlightReqs();

    // Helper method to get the number of migrations that have happened since
    // the planner was last reset
    int getNumMigrations();

    // Main entrypoint to request the execution of batches
    std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> callBatch(
      std::shared_ptr<BatchExecuteRequest> req);

    void callBatchWithoutLock(std::shared_ptr<BatchExecuteRequest> req);

    void enqueueCallBatch(std::shared_ptr<BatchExecuteRequest> req,
                          bool isChained = false);

    // ----------
    // Function State public API
    // ----------
    bool updateFuncParallelism(const std::string& userFunction,
                               int changedParallelism);

    bool resetBatchsize(int32_t newSize);

    bool resetMaxReplicas(int32_t newMaxReplicas);

    bool resetParameter(const std::string& key, const int32_t value);

    // ----------
    // Metrics public API
    // ----------
    // Get all Mertrics: Locking congestion time, processing time, etc.
    // MAP<Function, MAP<metric, value>>
    std::map<std::string, FunctionMetrics> collectMetrics();

  private:
    // There's a singleton instance of the planner running, but it must allow
    // concurrent requests
    std::shared_mutex plannerMx;

    PlannerState state;
    PlannerConfig config;

    faabric::util::ThreadSafeQueue<
      std::shared_ptr<faabric::BatchExecuteRequest>>
      batchExecuteReqQueue;

    // Check the waiting queue peroiodically.
    void batchTimerCheck();

    // ---- Batch Execution ----
    std::thread batchTimerThread;
    bool stopBatchTimer = false;

    bool streamMode = faabric::util::getSystemConfig().streamMode;
    long lastParallelismUpdate;
    int parallelismUpdateInterval;
    bool isPreloadParallelism;

    unsigned int chainedIdCounter = 0;
    // std::atomic<unsigned int> atomicChainedCounter{ 1 };

    // Snapshot registry to distribute snapshots in THREADS requests
    faabric::snapshot::SnapshotRegistry& snapshotRegistry;

    // ----------
    // Util private API
    // ----------

    void flushHosts();

    void flushExecutors();

    void flushSchedulingState();

    // ----------
    // Host membership private API
    // ----------

    // Check if a host's registration timestamp has expired
    bool isHostExpired(std::shared_ptr<Host> host, long epochTimeMs = 0);

    // ----------
    // Request scheduling private API
    // ----------

    void dispatchSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> decision);
};

Planner& getPlanner();
}
