#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/planner/Planner.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/batch.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <string>

namespace faabric::planner {

// ----------------------
// Utility Functions
// ----------------------

static void claimHostSlots(std::shared_ptr<Host> host, int slotsToClaim = 1)
{
    host->set_usedslots(host->usedslots() + slotsToClaim);
    assert(host->usedslots() <= host->slots());
}

static void releaseHostSlots(std::shared_ptr<Host> host, int slotsToRelease = 1)
{
    host->set_usedslots(host->usedslots() - slotsToRelease);
    assert(host->usedslots() >= 0);
}

static void printHostState(std::map<std::string, std::shared_ptr<Host>> hostMap,
                           const std::string& logLevel = "debug")
{
    std::string printedText;
    std::string header = "\n-------------- Host Map --------------";
    std::string subhead = "Ip\t\tSlots";
    std::string footer = "--------------------------------------";

    printedText += header + "\n" + subhead + "\n";
    for (const auto& [ip, hostState] : hostMap) {
        printedText += fmt::format(
          "{}\t{}/{}\n", ip, hostState->usedslots(), hostState->slots());
    }
    printedText += footer;

    if (logLevel == "debug") {
        SPDLOG_DEBUG(printedText);
    } else if (logLevel == "info") {
        SPDLOG_INFO(printedText);
    } else if (logLevel == "warn") {
        SPDLOG_WARN(printedText);
    } else if (logLevel == "error") {
        SPDLOG_ERROR(printedText);
    } else {
        SPDLOG_ERROR("Unrecognised log level: {}", logLevel);
    }
}

// ----------------------
// Planner
// ----------------------

// Planner is used globally as a static variable. This constructor relies on
// the fact that C++ static variable's initialisation is thread-safe
Planner::Planner()
  : snapshotRegistry(faabric::snapshot::getSnapshotRegistry())
{
    // Note that we don't initialise the config in a separate method to prevent
    // that method from being called elsewhere in the codebase (as it would be
    // thread-unsafe)
    config.set_ip(faabric::util::getSystemConfig().endpointHost);
    config.set_hosttimeout(std::stoi(
      faabric::util::getEnvVar("PLANNER_HOST_KEEPALIVE_TIMEOUT", "5")));
    config.set_numthreadshttpserver(
      std::stoi(faabric::util::getEnvVar("PLANNER_HTTP_SERVER_THREADS", "20")));

    printConfig();

    // Register the information for parallelism scaling
    lastParallelismUpdate = faabric::util::getGlobalClock().epochMillis();
    parallelismUpdateInterval =
      faabric::util::getSystemConfig().parallelismUpdateInterval;
    isPreloadParallelism = faabric::util::getSystemConfig().preloadParallelism;

    batchTimerThread = std::thread(&Planner::batchTimerCheck, this);
}

Planner::~Planner()
{
    // Stop the batch timer thread
    stopBatchTimer = true;
    if (batchTimerThread.joinable()) {
        batchTimerThread.join();
    }
}

PlannerConfig Planner::getConfig()
{
    return config;
}

void Planner::printConfig() const
{
    SPDLOG_INFO("--- Planner Conifg ---");
    SPDLOG_INFO("HOST_KEEP_ALIVE_TIMEOUT    {}", config.hosttimeout());
    SPDLOG_INFO("HTTP_SERVER_THREADS        {}", config.numthreadshttpserver());
}

bool Planner::reset()
{
    SPDLOG_INFO("Resetting planner");

    flushSchedulingState();

    flushHosts();

    return true;
}

bool Planner::flush(faabric::planner::FlushType flushType)
{
    switch (flushType) {
        case faabric::planner::FlushType::Hosts:
            SPDLOG_INFO("Planner flushing available hosts state");
            flushHosts();
            return true;
        case faabric::planner::FlushType::Executors:
            SPDLOG_INFO("Planner flushing executors");
            flushExecutors();
            return true;
        case faabric::planner::FlushType::SchedulingState:
            SPDLOG_INFO("Planner flushing scheduling state");
            flushSchedulingState();
            return true;
        default:
            SPDLOG_ERROR("Unrecognised flush type");
            return false;
    }
}

void Planner::flushHosts()
{
    faabric::util::FullLock lock(plannerMx);

    state.hostMap.clear();
}

void Planner::flushExecutors()
{
    // Flush Planner State
    state.funcLatencyStats.clear();
    state.chainFuncLatencyStats.clear();
    state.chainedInflights.clear();
    // Flush Aware Scheduler State
    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    auto stateAwareScheduler =
      std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
        batchScheduler);
    // If preload the parallelism desision, we change the parallelism here
    if (stateAwareScheduler) {
        stateAwareScheduler->flushStateInfo();
    }

    auto availableHosts = getAvailableHosts();

    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner sending EXECUTOR flush to {}", host->ip());
        faabric::scheduler::getFunctionCallClient(host->ip())->sendFlush();
    }
}

void Planner::flushSchedulingState()
{
    faabric::util::FullLock lock(plannerMx);

    state.inFlightReqs.clear();
    state.appResults.clear();
    state.appResultWaiters.clear();
    state.numMigrations = 0;
}

std::vector<std::shared_ptr<Host>> Planner::getAvailableHosts()
{
    SPDLOG_DEBUG("Planner received request to get available hosts");

    // Acquire a full lock because we will also remove the hosts that have
    // timed out
    faabric::util::FullLock lock(plannerMx);

    std::vector<std::string> hostsToRemove;
    std::vector<std::shared_ptr<Host>> availableHosts;
    auto timeNowMs = faabric::util::getGlobalClock().epochMillis();
    for (const auto& [ip, host] : state.hostMap) {
        if (isHostExpired(host, timeNowMs)) {
            hostsToRemove.push_back(ip);
        } else {
            availableHosts.push_back(host);
        }
    }

    for (const auto& host : hostsToRemove) {
        state.hostMap.erase(host);
    }

    return availableHosts;
}

// Deliberately take a const reference as an argument to force a copy and take
// ownership of the host
bool Planner::registerHost(const Host& hostIn, bool overwrite)
{
    SPDLOG_TRACE("Planner received request to register host {}", hostIn.ip());

    // Sanity check the input argument
    if (hostIn.slots() < 0) {
        SPDLOG_ERROR(
          "Received erroneous request to register host {} with {} slots",
          hostIn.ip(),
          hostIn.slots());
        return false;
    }

    faabric::util::FullLock lock(plannerMx);

    auto it = state.hostMap.find(hostIn.ip());
    if (it == state.hostMap.end() || isHostExpired(it->second)) {
        // If the host entry has expired, we remove it and treat the host
        // as a new one
        if (it != state.hostMap.end()) {
            state.hostMap.erase(it);
        }

        // If its the first time we see this IP, give it a UID and add it to
        // the map
        SPDLOG_INFO(
          "Registering host {} with {} slots", hostIn.ip(), hostIn.slots());
        state.hostMap.emplace(
          std::make_pair<std::string, std::shared_ptr<Host>>(
            (std::string)hostIn.ip(), std::make_shared<Host>(hostIn)));
    } else if (it != state.hostMap.end() && overwrite) {
        // We allow overwritting the host state by sending another register
        // request with same IP but different host resources. This is useful
        // for testing and resetting purposes
        SPDLOG_INFO("Overwritting host {} with {} slots (used {})",
                    hostIn.ip(),
                    hostIn.slots(),
                    hostIn.usedslots());
        it->second->set_slots(hostIn.slots());
        it->second->set_usedslots(hostIn.usedslots());
    } else if (it != state.hostMap.end()) {
        SPDLOG_TRACE("NOT overwritting host {} with {} slots (used {})",
                     hostIn.ip(),
                     hostIn.slots(),
                     hostIn.usedslots());
    }

    // Irrespective, set the timestamp
    SPDLOG_TRACE("Setting timestamp for host {}", hostIn.ip());
    state.hostMap.at(hostIn.ip())
      ->mutable_registerts()
      ->set_epochms(faabric::util::getGlobalClock().epochMillis());

    return true;
}

void Planner::removeHost(const Host& hostIn)
{
    SPDLOG_INFO("Planner received request to remove host {}", hostIn.ip());

    // We could acquire first a read lock to see if the host is in the host
    // map, and then acquire a write lock to remove it, but we don't do it
    // as we don't expect that much throughput in the planner
    faabric::util::FullLock lock(plannerMx);

    auto it = state.hostMap.find(hostIn.ip());
    if (it != state.hostMap.end()) {
        SPDLOG_DEBUG("Planner removing host {}", hostIn.ip());
        state.hostMap.erase(it);
    }
}

bool Planner::isHostExpired(std::shared_ptr<Host> host, long epochTimeMs)
{
    // Allow calling the method without a timestamp, and we calculate it now
    if (epochTimeMs == 0) {
        epochTimeMs = faabric::util::getGlobalClock().epochMillis();
    }

    long hostTimeoutMs = getConfig().hosttimeout() * 1000;
    return (epochTimeMs - host->registerts().epochms()) > hostTimeoutMs;
}

void Planner::setMessageResult(std::shared_ptr<faabric::Message> msg,
                               bool locked)
{
    int appId = msg->appid();
    int msgId = msg->id();

    // If we are dealing with a migrated message, we can ignore the setting
    // of the message result. This is because the migrated function will
    // have the same message id, and thus we will call this method again with
    // a message with the same message id. In addition, all the in-flihght
    // state is updated accordingly when we schedule the migration
    bool isMigratedMsg = msg->returnvalue() == MIGRATED_FUNCTION_RETURN_VALUE;
    if (isMigratedMsg) {
        return;
    }

    faabric::util::FullLock lock;
    if (!locked) {
        lock = faabric::util::FullLock(plannerMx);
    }

    SPDLOG_DEBUG("Planner setting message result (id: {}) for {}:{}:{}",
                 msg->id(),
                 msg->appid(),
                 msg->groupid(),
                 msg->groupidx());

    // Release the slot only once
    assert(state.hostMap.contains(msg->executedhost()));
    if (!state.appResults[appId].contains(msgId)) {
        releaseHostSlots(state.hostMap.at(msg->executedhost()));
    }

    // Set the result
    state.appResults[appId][msgId] = msg;

    // Remove the message from the in-flight requests
    if (!state.inFlightReqs.contains(appId)) {
        // We don't want to error if any client uses `setMessageResult`
        // liberally. This means that it may happen that when we set a message
        // result a second time, the app is already not in-flight
        SPDLOG_DEBUG("Setting result for non-existant (or finished) app: {}",
                     appId);
    } else {
        auto req = state.inFlightReqs.at(appId).first;
        auto decision = state.inFlightReqs.at(appId).second;

        // Work out the message position in the BER
        auto it = std::find_if(
          req->messages().begin(), req->messages().end(), [&](auto innerMsg) {
              return innerMsg.id() == msg->id();
          });
        if (it == req->messages().end()) {
            // Ditto as before. We want to allow setting the message result
            // more than once without breaking
            SPDLOG_DEBUG(
              "Setting result for non-existant (or finished) message: {}",
              appId);
        } else {
            SPDLOG_DEBUG("Removing message {} from app {}", msg->id(), appId);

            // Remove message from in-flight requests
            req->mutable_messages()->erase(it);

            // Remove message from decision
            decision->removeMessage(msg->id());

            // Record the Metrics
            std::string userFunc = msg->user() + "_" + msg->function();
            int parallelismId = msg->parallelismid();
            std::string userFuncPar =
              userFunc + "_" + std::to_string(parallelismId);
            if (state.funcLatencyStats.find(userFuncPar) !=
                state.funcLatencyStats.end()) {
                // Record the Lock metrics here
                int waitingTime = msg->workerpoptime() - msg->workerqueuetime();
                int executeTime =
                  msg->finishtimestamp() - msg->starttimestamp();
                state.funcLatencyStats[userFuncPar]->removeInFlightReqs(
                  msgId, waitingTime, executeTime);
                // Print the metrics Only for debug now.
                // state.funcLatencyStats[userFuncPar]->print();
            }

            // Record the Chain metrics
            int chainedId = msg->chainedid();
            state.chainedInflights[chainedId]--;
            if (state.chainedInflights[chainedId] == 0) {
                // req is the first message in the chain
                userFunc = req->user() + "_" + req->function();
                if (state.chainFuncLatencyStats.find(userFunc) !=
                    state.chainFuncLatencyStats.end()) {
                    state.chainFuncLatencyStats[userFunc]->removeInFlightReqs(
                      chainedId);
                    // state.chainFuncLatencyStats[userFunc]->print();
                }
                state.chainedInflights.erase(chainedId);
            }

            // Remove pair altogether if no more messages left
            if (req->messages_size() == 0) {
                SPDLOG_INFO("Planner removing app {} from in-flight", appId);
                assert(decision->nFunctions == 0);
                assert(decision->hosts.empty());
                assert(decision->messageIds.empty());
                assert(decision->appIdxs.empty());
                assert(decision->groupIdxs.empty());
                state.inFlightReqs.erase(appId);

                // If we are removing the app from in-flight, we can also
                // remmove any pre-loaded scheduling decisions
                if (state.preloadedSchedulingDecisions.contains(appId)) {
                    SPDLOG_DEBUG(
                      "Removing preloaded scheduling decision for app {}",
                      appId);
                    state.preloadedSchedulingDecisions.erase(appId);
                }
            }
        }
    }

    // Finally, dispatch an async message to all hosts that are waiting once
    // all planner accounting is updated
    if (state.appResultWaiters.find(msgId) != state.appResultWaiters.end()) {
        for (const auto& host : state.appResultWaiters[msgId]) {
            SPDLOG_DEBUG("Sending result to waiting host: {}", host);
            faabric::scheduler::getFunctionCallClient(host)->setMessageResult(
              msg);
        }
    }
}

void Planner::setMessageResultWitoutLock(std::shared_ptr<faabric::Message> msg)
{
    int appId = msg->appid();
    int msgId = msg->id();

    faabric::util::FullLock lock(plannerMx);

    SPDLOG_TRACE("Planner setting message result (id: {}) for {}:{}:{}",
                 msg->id(),
                 msg->appid(),
                 msg->groupid(),
                 msg->groupidx());

    // Release the slot only once
    assert(state.hostMap.contains(msg->executedhost()));

    // Set the result
    state.appResults[appId][msgId] = msg;

    if (!state.inFlightReqs.contains(appId)) {
        // We don't want to error if any client uses `setMessageResult`
        // liberally. This means that it may happen that when we set a message
        // result a second time, the app is already not in-flight
        SPDLOG_DEBUG("Setting result for non-existant (or finished) app: {}",
                     appId);
    } else {
        auto req = state.inFlightReqs.at(appId).first;
        auto decision = state.inFlightReqs.at(appId).second;

        // Work out the message position in the BER
        auto it = std::find_if(
          req->messages().begin(), req->messages().end(), [&](auto innerMsg) {
              return innerMsg.id() == msg->id();
          });
        if (it == req->messages().end()) {
            // Ditto as before. We want to allow setting the message result
            // more than once without breaking
            SPDLOG_DEBUG(
              "Setting result for non-existant (or finished) message: {}",
              appId);
        } else {
            SPDLOG_TRACE("Removing message {} from app {}", msg->id(), appId);

            // Remove message from in-flight requests
            req->mutable_messages()->erase(it);

            // Remove message from decision
            decision->removeMessage(msg->id());

            // Record the Metrics
            std::string userFunc = msg->user() + "_" + msg->function();
            int parallelismId = msg->parallelismid();
            std::string userFuncPar =
              userFunc + "_" + std::to_string(parallelismId);
            if (state.funcLatencyStats.find(userFuncPar) !=
                state.funcLatencyStats.end()) {
                // Record the Lock metrics here
                // int plannerQueueTime =
                //   msg->plannerpoptime() - msg->plannerqueuetime();
                // int plannerConsumeTime = msg->plannerdispatchtime() -
                //                          msg->workerqueuetime();
                int workerQueueTime = msg->workerpoptime() - msg->workerqueuetime();
                // int prepareExecuterTime = msg->executorpreparetime();
                int executeTime =
                  msg->workerexecuteend() - msg->workerexecutestart();
                // int totalTime = msg->finishtimestamp() - msg->starttimestamp();
                state.funcLatencyStats[userFuncPar]->removeInFlightReqs(
                  msgId, workerQueueTime, executeTime);
                // Print the metrics Only for debug now.
                // state.funcLatencyStats[userFuncPar]->print();
            }

            // Record the Chain metrics
            int chainedId = msg->chainedid();
            state.chainedInflights[chainedId]--;
            if (state.chainedInflights[chainedId] == 0) {
                // req is the first message in the chain
                userFunc = req->user() + "_" + req->function();
                if (state.chainFuncLatencyStats.find(userFunc) !=
                    state.chainFuncLatencyStats.end()) {
                    state.chainFuncLatencyStats[userFunc]->removeInFlightReqs(
                      chainedId);
                    // state.chainFuncLatencyStats[userFunc]->print();
                }
                state.chainedInflights.erase(chainedId);
            }

            // Remove pair altogether if no more messages left
            if (req->messages_size() == 0) {
                state.inFlightReqs.erase(appId);
            }
        }
    }
    // Finally, dispatch an async message to all hosts that are waiting once
    // all planner accounting is updated
    if (state.appResultWaiters.find(msgId) != state.appResultWaiters.end()) {
        for (const auto& host : state.appResultWaiters[msgId]) {
            SPDLOG_DEBUG("Sending result to waiting host: {}", host);
            faabric::scheduler::getFunctionCallClient(host)->setMessageResult(
              msg);
        }
    }
}

void Planner::setMessageResultBatch(
  std::shared_ptr<faabric::BatchExecuteRequest> batchMsg)
{
    for (int msgIdx = 0; msgIdx < batchMsg->messages_size(); msgIdx++) {
        auto msg = batchMsg->messages(msgIdx);
        setMessageResultWitoutLock(std::make_shared<faabric::Message>(msg));
    }
}

std::shared_ptr<faabric::Message> Planner::getMessageResult(
  std::shared_ptr<faabric::Message> msg)
{
    int appId = msg->appid();
    int msgId = msg->id();

    {
        faabric::util::SharedLock lock(plannerMx);

        // We debug and not error these messages as they happen frequently
        // when polling for results
        if (state.appResults.find(appId) == state.appResults.end()) {
            SPDLOG_DEBUG("App {} not registered in app results", appId);
        } else if (state.appResults[appId].find(msgId) ==
                   state.appResults[appId].end()) {
            SPDLOG_DEBUG("Msg {} not registered in app results (app id: {})",
                         msgId,
                         appId);
        } else {
            return state.appResults[appId][msgId];
        }
    }

    // If we are here, it means that we have not found the message result, so
    // we register the calling-host's interest if the calling-host has
    // provided a main host. The main host is set when dispatching a message
    // within faabric, but not when sending an HTTP request
    if (!msg->mainhost().empty()) {
        faabric::util::FullLock lock(plannerMx);

        // Check again if the result is not set, as it could have been set
        // between releasing the shared lock and acquiring the full lock
        if (state.appResults.contains(appId) &&
            state.appResults[appId].contains(msgId)) {
            return state.appResults[appId][msgId];
        }

        // Definately the message result is not set, so we add the host to the
        // waiters list
        SPDLOG_DEBUG("Adding host {} on the waiting list for message {}",
                     msg->mainhost(),
                     msgId);
        state.appResultWaiters[msgId].push_back(msg->mainhost());
    }

    return nullptr;
}

void Planner::preloadSchedulingDecision(
  int32_t appId,
  std::shared_ptr<batch_scheduler::SchedulingDecision> decision)
{
    faabric::util::FullLock lock(plannerMx);

    if (state.preloadedSchedulingDecisions.contains(appId)) {
        SPDLOG_ERROR(
          "ERROR: preloaded scheduling decisions already contain app {}",
          appId);
        return;
    }

    SPDLOG_INFO("Pre-loading scheduling decision for app {}", appId);
    state.preloadedSchedulingDecisions[appId] = decision;
}

std::shared_ptr<batch_scheduler::SchedulingDecision>
Planner::getPreloadedSchedulingDecision(
  int32_t appId,
  std::shared_ptr<BatchExecuteRequest> ber)
{
    SPDLOG_DEBUG("Requesting pre-loaded scheduling decision for app {}", appId);
    // WARNING: this method is currently only called from the main Planner
    // entrypoint (callBatch) which has a FullLock, thus we don't need to
    // acquire a (SharedLock) here. In general, we would need a read-lock
    // to read the dict from the planner's state
    auto decision = state.preloadedSchedulingDecisions.at(appId);
    assert(decision != nullptr);

    // Only include in the returned scheduling decision the group indexes that
    // are in this BER. This can happen when consuming a preloaded decision
    // in two steps (e.g. for MPI)
    std::shared_ptr<batch_scheduler::SchedulingDecision> filteredDecision =
      std::make_shared<batch_scheduler::SchedulingDecision>(decision->appId,
                                                            decision->groupId);
    for (const auto& msg : ber->messages()) {
        int groupIdx = msg.groupidx();
        int idxInDecision = std::distance(decision->groupIdxs.begin(),
                                          std::find(decision->groupIdxs.begin(),
                                                    decision->groupIdxs.end(),
                                                    groupIdx));
        assert(idxInDecision < decision->groupIdxs.size());

        // Add the schedulign for this group idx to the filtered decision.
        // Make sure we also maintain the message IDs that come from the BER
        // (as we can not possibly predict them in the preloaded decision)
        filteredDecision->addMessage(decision->hosts.at(idxInDecision),
                                     msg.id(),
                                     decision->appIdxs.at(idxInDecision),
                                     decision->groupIdxs.at(idxInDecision));
    }
    assert(filteredDecision->hosts.size() == ber->messages_size());

    return filteredDecision;
}

std::shared_ptr<faabric::BatchExecuteRequestStatus> Planner::getBatchResults(
  int32_t appId)
{
    auto berStatus = faabric::util::batchExecStatusFactory(appId);

    // Acquire a read lock to copy all the results we have for this batch
    {
        faabric::util::SharedLock lock(plannerMx);

        if (!state.appResults.contains(appId)) {
            return nullptr;
        }

        for (auto msgResultPair : state.appResults.at(appId)) {
            *berStatus->add_messageresults() = *(msgResultPair.second);
        }

        // Set the finished condition
        berStatus->set_finished(!state.inFlightReqs.contains(appId));

        // WARNING: It might affect the get message result function and the
        // ExecGraph function. But in stream processing, it is not a problem.
        // Clean the queried results.
        if (berStatus->finished()) {
            state.appResults.erase(appId);
        }
    }

    return berStatus;
}

std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
Planner::getSchedulingDecision(std::shared_ptr<BatchExecuteRequest> req)
{
    int appId = req->appid();

    // Acquire a read lock to get the scheduling decision for the requested app
    faabric::util::SharedLock lock(plannerMx);

    if (state.inFlightReqs.find(appId) == state.inFlightReqs.end()) {
        return nullptr;
    }

    return state.inFlightReqs.at(appId).second;
}

faabric::batch_scheduler::InFlightReqs Planner::getInFlightReqs()
{
    faabric::util::SharedLock lock(plannerMx);

    // Deliberately deep copy here
    faabric::batch_scheduler::InFlightReqs inFlightReqsCopy;
    for (const auto& [appId, inFlightPair] : state.inFlightReqs) {
        inFlightReqsCopy[appId] = std::make_pair(
          std::make_shared<BatchExecuteRequest>(*inFlightPair.first),
          std::make_shared<faabric::batch_scheduler::SchedulingDecision>(
            *inFlightPair.second));
    }

    return inFlightReqsCopy;
}

int Planner::getNumMigrations()
{
    return state.numMigrations.load(std::memory_order_acquire);
}

static faabric::batch_scheduler::HostMap convertToBatchSchedHostMap(
  std::map<std::string, std::shared_ptr<Host>> hostMapIn)
{
    faabric::batch_scheduler::HostMap hostMap;

    for (const auto& [ip, host] : hostMapIn) {
        hostMap[ip] = std::make_shared<faabric::batch_scheduler::HostState>(
          host->ip(), host->slots(), host->usedslots());
    }

    return hostMap;
}

void Planner::enqueueCallBatch(std::shared_ptr<BatchExecuteRequest> req,
                               bool isChained)
{
    int appId = req->appid();

    // Record planner enqueue time
    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_plannerqueuetime(currentTime);
    }

    if (isChained) {
        faabric::util::FullLock lock(plannerMx);

        // If the message is chained, we need to record it to the state inflight
        // and chained count at first. Otherwise, the caller function might
        // finish before this function is recorded. In this case, the set batch
        // result might mark this chained functions are finished.
        auto oldReq = state.inFlightReqs.at(appId).first;
        auto oldDec = state.inFlightReqs.at(appId).second;

        for (size_t i = 0; i < req->messages_size(); i++) {
            *oldReq->add_messages() = req->messages(i);
            oldDec->addMessage("enqueue_not_set", req->messages(i));

            faabric::Message tempMessage = req->messages(i);
            int chainedid = tempMessage.chainedid();
            state.chainedInflights[chainedid]++;
        }
    }
    return batchExecuteReqQueue.enqueue(req);
}

void Planner::batchTimerCheck()
{
    while (!stopBatchTimer) {
        // If empty, this thread will be blocked.
        std::shared_ptr<BatchExecuteRequest> req =
          batchExecuteReqQueue.dequeue();
        callBatchWithoutLock(req);
    }
}

void Planner::callBatchWithoutLock(std::shared_ptr<BatchExecuteRequest> req)
{
    int appId = req->appid();

    faabric::util::FullLock lock(plannerMx);

    // Record planner dequeue time
    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_plannerpoptime(currentTime);
    }

    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    auto decisionType =
      batchScheduler->getDecisionType(state.inFlightReqs, req);

    // For the NEW messages, we assgin a chainedId for it if it is not set.
    // The chainId is used to trace the chain function.
    bool isNew = decisionType == faabric::batch_scheduler::DecisionType::NEW;
    if (isNew) {
        // Assign chainedId for the new messages
        for (int i = 0; i < req->messages_size(); i++) {
            // Assign chainedId for the new messages
            faabric::Message* tempMessage = req->mutable_messages(i);
            int chainedId = ++chainedIdCounter;
            tempMessage->set_chainedid(chainedId);
            SPDLOG_TRACE("Assign chained id {} for message {}",
                         chainedId,
                         tempMessage->id());
        }
    }

    std::shared_ptr<batch_scheduler::StateAwareScheduler> stateAwareScheduler =
      std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
        batchScheduler);

    if (!stateAwareScheduler) {
        throw std::runtime_error(
          "enqueue call batch is only supported by state aware scheduler");
    }

    // In stream processing, only the NEW and SCALE_CHANGE are supported.
    if (decisionType == faabric::batch_scheduler::DecisionType::DIST_CHANGE) {
        throw std::runtime_error(
          "Dist change is not supported in the stream mode");
    }

    auto hostMapCopy = convertToBatchSchedHostMap(state.hostMap);

    std::shared_ptr<batch_scheduler::SchedulingDecision> decision = nullptr;

    // TODO - This function should be removed. The default parallelism of each
    // function state is 1 and adjusted by the state-aware scheduler or
    // faasmctl.
    // if (isPreloadParallelism) {
    //     stateAwareScheduler->preloadParallelism(hostMapCopy);
    // } else {
    //     // TODO - adjust parallelism according to metrics
    // }

    decision = stateAwareScheduler->scheduleWithoutLock(
      hostMapCopy, state.inFlightReqs, req);

    assert(decision != nullptr);
    switch (decisionType) {
        case faabric::batch_scheduler::DecisionType::NEW: {

            // 1. For a new decision, we just add it to the in-flight map
            state.inFlightReqs[appId] = std::make_pair(req, decision);
            // 2 For a new decision, we record its metrics
            // Metrics include chained metrics and function metrics (BOTH
            // message level)
            for (size_t i = 0; i < req->messages_size(); i++) {
                faabric::Message tempMessage = req->messages(i);
                int tempParallelisimId = tempMessage.parallelismid();
                int msgId = tempMessage.id();
                int chainedId = tempMessage.chainedid();
                std::string userFunc =
                  tempMessage.user() + "_" + tempMessage.function();
                std::string userFuncPar =
                  userFunc + "_" + std::to_string(tempParallelisimId);

                if (state.chainFuncLatencyStats.find(userFunc) ==
                    state.chainFuncLatencyStats.end()) {
                    // The chain function name does not exist in the map, so
                    // we need to create a new Metrics object
                    std::shared_ptr<FunctionLatency> newMetrics =
                      std::make_shared<FunctionLatency>(userFunc);

                    // Finally, add the newMetrics object to the map
                    state.chainFuncLatencyStats[userFunc] =
                      std::move(newMetrics);
                }
                // For the NEW messages, reset the in-flight requests
                state.chainedInflights[chainedId] = 1;
                state.chainFuncLatencyStats[userFunc]->addInFlightReq(
                  chainedId);

                if (state.funcLatencyStats.find(userFuncPar) ==
                    state.funcLatencyStats.end()) {
                    // The function name does not exist in the map, so we
                    // need to create a new Metrics object
                    std::shared_ptr<FunctionLatency> newMetrics =
                      std::make_shared<FunctionLatency>(userFuncPar);
                    state.funcLatencyStats[userFuncPar] = std::move(newMetrics);
                }
                state.funcLatencyStats[userFuncPar]->addInFlightReq(msgId);
            }
            break;
        }
        case faabric::batch_scheduler::DecisionType::SCALE_CHANGE: {
            // 1. TODO - Up date the enqueue_not_set deceision

            // 2. Record the metrics for each function.
            for (size_t i = 0; i < req->messages_size(); i++) {
                faabric::Message tempMessage = req->messages(i);
                std::string userFunc =
                  tempMessage.user() + "_" + tempMessage.function();
                int tempParallelisimId = tempMessage.parallelismid();
                int msgId = tempMessage.id();
                std::string userFuncPar =
                  userFunc + "_" + std::to_string(tempParallelisimId);

                if (state.funcLatencyStats.find(userFuncPar) ==
                    state.funcLatencyStats.end()) {
                    // The function name does not exist in the map, so we
                    // need to create a new Metrics object
                    std::shared_ptr<FunctionLatency> newMetrics =
                      std::make_shared<FunctionLatency>(userFuncPar);
                    state.funcLatencyStats[userFuncPar] = std::move(newMetrics);
                }
                state.funcLatencyStats[userFuncPar]->addInFlightReq(msgId);
            }
            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised decision type: {} (app: {})",
                         decisionType,
                         req->appid());
            throw std::runtime_error("Unrecognised decision type");
        }
    }
    // Sanity-checks before actually dispatching functions for execution
    assert(req->messages_size() == decision->hosts.size());
    assert(req->appid() == decision->appId);

    dispatchSchedulingDecision(req, decision);
}

std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
Planner::callBatch(std::shared_ptr<BatchExecuteRequest> req)
{
    int appId = req->appid();

    // Acquire a full lock to make the scheduling decision and update the
    // in-filght map if necessary
    faabric::util::FullLock lock(plannerMx);

    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    auto decisionType =
      batchScheduler->getDecisionType(state.inFlightReqs, req);

    // For the NEW messages, we assgin a chainedId for it if it is not set.
    // The chainId is used to trace the chain function.
    bool isNew = decisionType == faabric::batch_scheduler::DecisionType::NEW;
    if (isNew) {
        // Assign chainedId for the new messages
        for (int i = 0; i < req->messages_size(); i++) {
            // Assign chainedId for the new messages
            faabric::Message* tempMessage = req->mutable_messages(i);
            // If the chainedId is not set, we assign a new chainedId for it.
            if (tempMessage->chainedid() != 0) {
                continue;
            }
            int chainedId = ++chainedIdCounter;
            tempMessage->set_chainedid(chainedId);
            SPDLOG_TRACE("Assign chained id {} for message {}",
                         chainedId,
                         tempMessage->id());
        }
    }

    // Make a copy of the host-map state to make sure the scheduling process
    // does not modify it
    auto hostMapCopy = convertToBatchSchedHostMap(state.hostMap);
    bool isDistChange =
      decisionType == faabric::batch_scheduler::DecisionType::DIST_CHANGE;

    // For a DIST_CHANGE decision (i.e. migration) we want to try to imrpove
    // on the old decision (we don't care the one we send), so we make sure
    // we are scheduling the same messages from the old request
    if (isDistChange) {
        SPDLOG_INFO("App {} asked for migration opportunities", appId);
        auto oldReq = state.inFlightReqs.at(appId).first;
        req->clear_messages();
        for (const auto& msg : oldReq->messages()) {
            *req->add_messages() = msg;
        }
    }

    // Check if there exists a pre-loaded scheduling decision for this app
    // (e.g. if we want to force a migration). Note that we don't want to check
    // pre-loaded decisions for dist-change requests
    std::shared_ptr<batch_scheduler::SchedulingDecision> decision = nullptr;
    if (!isDistChange && state.preloadedSchedulingDecisions.contains(appId)) {
        decision = getPreloadedSchedulingDecision(appId, req);
    } else {
        // For state-aware scheduler, according to metrics, adjust the
        // parallelism if necessary.
        std::shared_ptr<batch_scheduler::StateAwareScheduler>
          stateAwareScheduler =
            std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
              batchScheduler);
        // If preload the parallelism desision, we fix the parallelism
        // Otherwise adjust the parallelism according to the metrics
        if (stateAwareScheduler && !isPreloadParallelism) {
            long currentTime = faabric::util::getGlobalClock().epochMillis();
            if (currentTime - lastParallelismUpdate >
                parallelismUpdateInterval) {
                lastParallelismUpdate = currentTime;
                stateAwareScheduler->updateParallelism(hostMapCopy,
                                                       collectMetrics());
            }
        }
        decision = batchScheduler->makeSchedulingDecision(
          hostMapCopy, state.inFlightReqs, req);
    }
    assert(decision != nullptr);

    // Handle failures to schedule work
    if (*decision == NOT_ENOUGH_SLOTS_DECISION) {
        SPDLOG_ERROR(
          "Not enough free slots to schedule app: {} (requested: {})",
          appId,
          req->messages_size());
        printHostState(state.hostMap, "error");
        return decision;
    }

    if (*decision == DO_NOT_MIGRATE_DECISION) {
        SPDLOG_INFO("Decided to not migrate app: {}", appId);
        return decision;
    }

    // A scheduling decision will create a new PTP mapping and, as a
    // consequence, a new group ID
    int newGroupId = faabric::util::generateGid();
    decision->groupId = newGroupId;
    faabric::util::updateBatchExecGroupId(req, newGroupId);

    // Given a scheduling decision, depending on the decision type, we want to:
    // 1. Update the host-map to reflect the new host occupation
    // 2. Update the in-flight map to include the new request
    // 3. Send the PTP mappings to all the hosts involved
    auto& broker = faabric::transport::getPointToPointBroker();
    switch (decisionType) {
        case faabric::batch_scheduler::DecisionType::NEW: {
            // 0. Log the decision in debug mode
#ifndef NDEBUG
            decision->print();
#endif

            // 1. For a new request, we only need to update the hosts
            // with the new messages being scheduled
            for (int i = 0; i < decision->hosts.size(); i++) {
                claimHostSlots(state.hostMap.at(decision->hosts.at(i)));
            }

            // 2. For a new decision, we just add it to the in-flight map
            state.inFlightReqs[appId] = std::make_pair(req, decision);

            // 2.5 For a new decision, we record its metrics
            // Metrics include chained metrics and function metrics (BOTH
            // message level)
            for (size_t i = 0; i < req->messages_size(); i++) {
                faabric::Message tempMessage = req->messages(i);
                int tempParallelisimId = tempMessage.parallelismid();
                int msgId = tempMessage.id();
                int chainedId = tempMessage.chainedid();
                std::string userFunc =
                  tempMessage.user() + "_" + tempMessage.function();
                std::string userFuncPar =
                  userFunc + "_" + std::to_string(tempParallelisimId);

                if (state.chainFuncLatencyStats.find(userFunc) ==
                    state.chainFuncLatencyStats.end()) {
                    // The chain function name does not exist in the map, so we
                    // need to create a new Metrics object
                    std::shared_ptr<FunctionLatency> newMetrics =
                      std::make_shared<FunctionLatency>(userFunc);

                    // Finally, add the newMetrics object to the map
                    state.chainFuncLatencyStats[userFunc] =
                      std::move(newMetrics);
                }
                // For the NEW messages, reset the in-flight requests
                state.chainedInflights[chainedId] = 1;
                state.chainFuncLatencyStats[userFunc]->addInFlightReq(
                  chainedId);

                if (state.funcLatencyStats.find(userFuncPar) ==
                    state.funcLatencyStats.end()) {
                    // The function name does not exist in the map, so we
                    // need to create a new Metrics object
                    std::shared_ptr<FunctionLatency> newMetrics =
                      std::make_shared<FunctionLatency>(userFuncPar);
                    state.funcLatencyStats[userFuncPar] = std::move(newMetrics);
                }
                state.funcLatencyStats[userFuncPar]->addInFlightReq(msgId);
            }

            // 3. We send the mappings to all the hosts involved
            if (!streamMode) {
                broker.setAndSendMappingsFromSchedulingDecision(*decision);
            }

            break;
        }
        case faabric::batch_scheduler::DecisionType::SCALE_CHANGE: {
            // 1. For a scale change request, we only need to update the hosts
            // with the _new_ messages being scheduled
            for (int i = 0; i < decision->hosts.size(); i++) {
                claimHostSlots(state.hostMap.at(decision->hosts.at(i)));
            }

            // 2. For a scale change request, we want to update the BER with the
            // _new_ messages we are adding
            auto oldReq = state.inFlightReqs.at(appId).first;
            auto oldDec = state.inFlightReqs.at(appId).second;
            faabric::util::updateBatchExecGroupId(oldReq, newGroupId);
            oldDec->groupId = newGroupId;

            for (int i = 0; i < req->messages_size(); i++) {
                *oldReq->add_messages() = req->messages(i);
                oldDec->addMessage(decision->hosts.at(i), req->messages(i));
            }

            // 2.5. Log the updated decision in debug mode
#ifndef NDEBUG
            oldDec->print();
#endif

            // 2.6 Record the metrics for each function.
            for (size_t i = 0; i < req->messages_size(); i++) {
                faabric::Message tempMessage = req->messages(i);
                std::string userFunc =
                  tempMessage.user() + "_" + tempMessage.function();
                int tempParallelisimId = tempMessage.parallelismid();
                int msgId = tempMessage.id();
                int chainedid = tempMessage.chainedid();
                std::string userFuncPar =
                  userFunc + "_" + std::to_string(tempParallelisimId);

                if (state.funcLatencyStats.find(userFuncPar) ==
                    state.funcLatencyStats.end()) {
                    // The function name does not exist in the map, so we
                    // need to create a new Metrics object
                    std::shared_ptr<FunctionLatency> newMetrics =
                      std::make_shared<FunctionLatency>(userFuncPar);
                    state.funcLatencyStats[userFuncPar] = std::move(newMetrics);
                }
                state.funcLatencyStats[userFuncPar]->addInFlightReq(msgId);
                state.chainedInflights[chainedid]++;
            }

            // 3. We want to send the mappings for the _updated_ decision,
            // including _all_ the messages (not just the ones that are being
            // added)
            if (!streamMode) {
                broker.setAndSendMappingsFromSchedulingDecision(*oldDec);
            }

            break;
        }
        case faabric::batch_scheduler::DecisionType::DIST_CHANGE: {
            auto oldReq = state.inFlightReqs.at(appId).first;
            auto oldDec = state.inFlightReqs.at(appId).second;

            // 0. For the time being, when migrating we always print both
            // decisions (old and new)
            SPDLOG_INFO("Decided to migrate app {}!", appId);
            SPDLOG_INFO("Old decision:");
            oldDec->print("info");
            SPDLOG_INFO("New decision:");
            decision->print("info");
            state.numMigrations += 1;

            // We want to let all hosts involved in the migration (not only
            // those in the new decision) that we are gonna migrate. For the
            // evicted hosts (those present in the old decision but not in the
            // new one) we need to send the mappings manually

            // Work out the evicted host set (unfortunately, couldn't come up
            // with a less verbose way to do it)
            std::vector<std::string> evictedHostsVec;
            std::vector<std::string> oldDecHosts = oldDec->hosts;
            std::sort(oldDecHosts.begin(), oldDecHosts.end());
            std::vector<std::string> newDecHosts = decision->hosts;
            std::sort(newDecHosts.begin(), newDecHosts.end());
            std::set_difference(oldDecHosts.begin(),
                                oldDecHosts.end(),
                                newDecHosts.begin(),
                                newDecHosts.end(),
                                std::back_inserter(evictedHostsVec));
            std::set<std::string> evictedHosts(evictedHostsVec.begin(),
                                               evictedHostsVec.end());

            // 1. We only need to update the hosts where both decisions differ
            assert(decision->hosts.size() == oldDec->hosts.size());
            for (int i = 0; i < decision->hosts.size(); i++) {
                if (decision->hosts.at(i) == oldDec->hosts.at(i)) {
                    continue;
                }

                releaseHostSlots(state.hostMap.at(oldDec->hosts.at(i)));
                claimHostSlots(state.hostMap.at(decision->hosts.at(i)));
            }

            // 2. For a DIST_CHANGE request (migration), we want to replace the
            // exsiting decision with the new one
            faabric::util::updateBatchExecGroupId(oldReq, newGroupId);
            state.inFlightReqs.at(appId) = std::make_pair(oldReq, decision);

            // 3. We want to sent the new scheduling decision to all the hosts
            // involved in the migration (even the ones that are evicted)
            if (!streamMode) {
                broker.setAndSendMappingsFromSchedulingDecision(*decision);
                broker.sendMappingsFromSchedulingDecision(*decision,
                                                          evictedHosts);
            }

            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised decision type: {} (app: {})",
                         decisionType,
                         req->appid());
            throw std::runtime_error("Unrecognised decision type");
        }
    }

    // Sanity-checks before actually dispatching functions for execution
    assert(req->messages_size() == decision->hosts.size());
    assert(req->appid() == decision->appId);
    assert(req->groupid() == decision->groupId);

    // Lastly, asynchronously dispatch the execute requests to the
    // corresponding hosts if new functions need to be spawned (not if
    // migrating)
    // We may not need the lock here anymore, but we are eager to make the
    // whole function atomic)
    if (decisionType != faabric::batch_scheduler::DecisionType::DIST_CHANGE) {
        dispatchSchedulingDecision(req, decision);
    }

    return decision;
}

void Planner::dispatchSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> decision)
{
    std::map<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>
      hostRequests;

    assert(req->messages_size() == decision->hosts.size());
    bool isSingleHost = decision->isSingleHost();

    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    // First we build all the BatchExecuteRequests for all the different hosts.
    // We need to keep a map as the hosts may not be contiguous in the decision
    // (i.e. we may have (hostA, hostB, hostA)
    for (int i = 0; i < req->messages_size(); i++) {
        auto mutable_msg = req->mutable_messages(i);
        mutable_msg->set_plannerdispatchtime(currentTime);
        auto msg = req->messages().at(i);

        // Initialise the BER if it is not there
        std::string thisHost = decision->hosts.at(i);
        if (hostRequests.find(thisHost) == hostRequests.end()) {
            hostRequests[thisHost] = faabric::util::batchExecFactory();
            hostRequests[thisHost]->set_appid(decision->appId);
            hostRequests[thisHost]->set_groupid(decision->groupId);
            hostRequests[thisHost]->set_user(msg.user());
            hostRequests[thisHost]->set_function(msg.function());
            hostRequests[thisHost]->set_snapshotkey(req->snapshotkey());
            hostRequests[thisHost]->set_type(req->type());
            hostRequests[thisHost]->set_subtype(req->subtype());
            hostRequests[thisHost]->set_contextdata(req->contextdata());
            hostRequests[thisHost]->set_singlehost(isSingleHost);
            // Propagate the single host hint
            hostRequests[thisHost]->set_singlehosthint(req->singlehosthint());
        }

        *hostRequests[thisHost]->add_messages() = msg;
    }

    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    if (!isSingleHost && req->singlehosthint()) {
        SPDLOG_ERROR(
          "User provided single-host hint in BER, but decision is not!");
    }

    for (const auto& [hostIp, hostReq] : hostRequests) {
        SPDLOG_DEBUG("Dispatching {} messages to host {} for execution",
                     hostReq->messages_size(),
                     hostIp);
        assert(faabric::util::isBatchExecRequestValid(hostReq));

        // In a THREADS request, before sending an execution request we need to
        // push the main (caller) thread snapshot to all non-main hosts
        // FIXME: ideally, we would do this from the caller thread, once we
        // know the scheduling decision and all other threads would be awaiting
        // for the snapshot
        if (isThreads && !isSingleHost) {
            auto snapshotKey =
              faabric::util::getMainThreadSnapshotKey(hostReq->messages(0));
            try {
                auto snap = snapshotRegistry.getSnapshot(snapshotKey);

                // TODO(thread-opt): push only diffs
                if (hostIp != req->messages(0).mainhost()) {
                    faabric::snapshot::getSnapshotClient(hostIp)->pushSnapshot(
                      snapshotKey, snap);
                }
            } catch (std::runtime_error& e) {
                // Catch errors, but don't let them crash the planner. Let the
                // worker crash instead
                SPDLOG_ERROR("Snapshot {} not regsitered in planner!",
                             snapshotKey);
            }
        }

        faabric::scheduler::getFunctionCallClient(hostIp)->executeFunctions(
          hostReq);
    }

    SPDLOG_DEBUG("Finished dispatching {} messages for execution",
                 req->messages_size());
}

std::map<std::string, FunctionMetrics> Planner::collectMetrics()
{
    // metrics states contains two types information: topology and function
    // Topology info is stored with source function name: UserFunction
    // Function info is stored with function name with parallelism: UserFuncPar
    std::map<std::string, FunctionMetrics> metricsStats;
    // Retrive the Latency Metrics from planner state
    // Metrics of chained functions (topology)
    for (auto [ithFunction, ithLatency] : state.chainFuncLatencyStats) {
        FunctionMetrics ithMetrics = FunctionMetrics(ithFunction);
        ithMetrics.isChained = true;
        ithMetrics.throughput = ithLatency->completedRequests;
        ithMetrics.processLatency = ithLatency->averageLatency;
        metricsStats.emplace(ithFunction, ithMetrics);
    }
    std::map<std::string, std::map<std::string, int>> tempMetrics;
    // Retrive Metrics from State Server.
    for (auto [ithHostName, ithHostObject] : state.hostMap) {
        state::FunctionStateClient cli(ithHostName);
        auto hostMetrics = cli.getMetrics();
        for (auto& [ithUserFuncPar, ithStateMetrics] : hostMetrics) {
            // Accumulate metrics
            tempMetrics[ithUserFuncPar][LOCK_BLOCK_TIME] +=
              ithStateMetrics[LOCK_BLOCK_TIME];
            tempMetrics[ithUserFuncPar][LOCK_HOLD_TIME] +=
              ithStateMetrics[LOCK_HOLD_TIME];
        }
    }

    // Metrics of single function
    for (auto [ithFunction, ithLatency] : state.funcLatencyStats) {
        FunctionMetrics ithMetrics = FunctionMetrics(ithFunction);
        ithMetrics.throughput = ithLatency->completedRequests;
        ithMetrics.processLatency = ithLatency->averageLatency;
        ithMetrics.averageWaitingTime = ithLatency->averageWaitingTime;
        ithMetrics.averageExecuteTime = ithLatency->averageExecuteTime;
        for (auto [ithUserFuncPar, ithStateMetrics] : tempMetrics) {
            if (ithUserFuncPar != ithMetrics.function) {
                continue;
            }
            ithMetrics.lockCongestionTime = ithStateMetrics[LOCK_BLOCK_TIME];
            ithMetrics.lockHoldTime = ithStateMetrics[LOCK_HOLD_TIME];
        }
        metricsStats.emplace(ithFunction, ithMetrics);
    }

    // Print the metrics.
    for (auto [ithFunc, ithMetrics] : metricsStats) {
        SPDLOG_DEBUG("Metrics for {}: throughput: {}, processLatency: {}, "
                     "averageWaitingTime: {}, lockCongestionTime: {}, "
                     "lockHoldTime: {}",
                     ithFunc,
                     ithMetrics.throughput,
                     ithMetrics.processLatency,
                     ithMetrics.averageWaitingTime,
                     ithMetrics.lockCongestionTime,
                     ithMetrics.lockHoldTime);
    }
    return metricsStats;
}

bool Planner::updateFuncParallelism(const std::string& userFunction,
                                    int changedParallelism)
{
    SPDLOG_DEBUG("Planner received request to changed parallelism for {} by {}",
                 userFunction,
                 changedParallelism);
    faabric::util::FullLock lock(plannerMx);
    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    std::shared_ptr<batch_scheduler::StateAwareScheduler> stateAwareScheduler =
      std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
        batchScheduler);
    if (!stateAwareScheduler) {
        SPDLOG_ERROR("State-aware scheduler is not enabled");
        return false;
    }
    auto hostMapCopy = convertToBatchSchedHostMap(state.hostMap);
    stateAwareScheduler->increaseFunctionParallelism(
      changedParallelism, userFunction, hostMapCopy);
    return true;
}

bool Planner::resetBatchsize(int32_t newSize)
{
    auto availableHosts = getAvailableHosts();
    faabric::planner::BatchResetRequest req;
    req.set_batchsize(newSize);

    for (const auto& host : availableHosts) {
        SPDLOG_INFO(
          "Planner resize the batchsize {} to {}", host->ip(), newSize);
        faabric::scheduler::getFunctionCallClient(host->ip())
          ->resetBatchSize(
            std::make_shared<faabric::planner::BatchResetRequest>(req));
    }

    return true;
}

bool Planner::resetMaxReplicas(int32_t maxReplicas)
{
    auto availableHosts = getAvailableHosts();
    faabric::planner::MaxReplicasRequest req;
    req.set_maxnum(maxReplicas);

    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner max replicas {} to {}", host->ip(), maxReplicas);
        faabric::scheduler::getFunctionCallClient(host->ip())
          ->resetMaxReplicas(
            std::make_shared<faabric::planner::MaxReplicasRequest>(req));
    }

    return true;
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
