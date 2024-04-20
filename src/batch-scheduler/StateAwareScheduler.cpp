#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
#include <faabric/util/serialization.h>
#include <faabric/util/string_tools.h>

#define MAIN_KEY_PREFIX "main_"

namespace faabric::batch_scheduler {

// We register the function state in the functionStateRegister map.
void StateAwareScheduler::funcStateInitializer(
  std::map<std::string, std::tuple<std::string, std::string>> otherRegister)
{
    funcStateRegMap["stream_function_state"] = std::make_tuple("", "");
    funcStateRegMap["stream_function_parstate"] =
      std::make_tuple("partitionInputKey", "partitionStateKey");
}

static std::map<std::string, int> getHostFreqCount(
  std::shared_ptr<SchedulingDecision> decision)
{
    std::map<std::string, int> hostFreqCount;
    for (auto host : decision->hosts) {
        hostFreqCount[host] += 1;
    }

    return hostFreqCount;
}

// Given a new decision that improves on an old decision (i.e. to migrate), we
// want to make sure that we minimise the number of migration requests we send.
// This is, we want to keep as many host-message scheduling in the old decision
// as possible, and also have the overall locality of the new decision (i.e.
// the host-message histogram)
static std::shared_ptr<SchedulingDecision> minimiseNumOfMigrations(
  std::shared_ptr<SchedulingDecision> newDecision,
  std::shared_ptr<SchedulingDecision> oldDecision)
{
    auto decision = std::make_shared<SchedulingDecision>(oldDecision->appId,
                                                         oldDecision->groupId);

    // We want to maintain the new decision's host-message histogram
    auto hostFreqCount = getHostFreqCount(newDecision);

    // Helper function to find the next host in the histogram with slots
    auto nextHostWithSlots = [&hostFreqCount]() -> std::string {
        for (auto [ip, slots] : hostFreqCount) {
            if (slots > 0) {
                return ip;
            }
        }

        // Unreachable (in this context)
        throw std::runtime_error("No next host with slots found!");
    };

    assert(newDecision->hosts.size() == oldDecision->hosts.size());
    for (int i = 0; i < newDecision->hosts.size(); i++) {
        // If both decisions schedule this message to the same host great, as
        // we can keep the old scheduling
        if (newDecision->hosts.at(i) == oldDecision->hosts.at(i) &&
            hostFreqCount.at(newDecision->hosts.at(i)) > 0) {
            decision->addMessage(oldDecision->hosts.at(i),
                                 oldDecision->messageIds.at(i),
                                 oldDecision->appIdxs.at(i),
                                 oldDecision->groupIdxs.at(i));
            hostFreqCount.at(oldDecision->hosts.at(i)) -= 1;
            continue;
        }

        // If not, assign the old decision as long as we still can (i.e. as
        // long as we still have slots in the histogram (note that it could be
        // that the old host is not in the new histogram at all)
        if (hostFreqCount.contains(oldDecision->hosts.at(i)) &&
            hostFreqCount.at(oldDecision->hosts.at(i)) > 0) {
            decision->addMessage(oldDecision->hosts.at(i),
                                 oldDecision->messageIds.at(i),
                                 oldDecision->appIdxs.at(i),
                                 oldDecision->groupIdxs.at(i));
            hostFreqCount.at(oldDecision->hosts.at(i)) -= 1;
            continue;
        }

        // If we can't assign the host from the old decision, then it means
        // that that message MUST be migrated, so it doesn't really matter
        // which of the hosts from the new migration we pick (as the new
        // decision is optimal in terms of bin-packing), as long as there are
        // still slots in the histogram
        auto nextHost = nextHostWithSlots();
        decision->addMessage(nextHost,
                             oldDecision->messageIds.at(i),
                             oldDecision->appIdxs.at(i),
                             oldDecision->groupIdxs.at(i));
        hostFreqCount.at(nextHost) -= 1;
    }

    // Assert that we have preserved the new decision's host-message histogram
    // (use the pre-processor macro as we assert repeatedly in the loop, so we
    // want to avoid having an empty loop in non-debug mode)
#ifndef NDEBUG
    for (auto [host, freq] : hostFreqCount) {
        assert(freq == 0);
    }
#endif

    return decision;
}

// For the BinPack scheduler, a decision is better than another one if it spans
// less hosts. In case of a tie, we calculate the number of cross-VM links
// (i.e. better locality, or better packing)
bool StateAwareScheduler::isFirstDecisionBetter(
  std::shared_ptr<SchedulingDecision> decisionA,
  std::shared_ptr<SchedulingDecision> decisionB)
{
    // The locality score is currently the number of cross-VM links. You may
    // calculate this number as follows:
    // - If the decision is single host, the number of cross-VM links is zero
    // - Otherwise, in a fully-connected graph, the number of cross-VM links
    //   is the sum of edges that cross a VM boundary
    auto getLocalityScore =
      [](std::shared_ptr<SchedulingDecision> decision) -> std::pair<int, int> {
        // First, calculate the host-message histogram (or frequency count)
        std::map<std::string, int> hostFreqCount;
        for (auto host : decision->hosts) {
            hostFreqCount[host] += 1;
        }

        // If scheduling is single host, return one host and 0 cross-host links
        if (hostFreqCount.size() == 1) {
            return std::make_pair(1, 0);
        }

        // Else, sum all the egressing edges for each element and divide by two
        int score = 0;
        for (auto [host, freq] : hostFreqCount) {

            int thisHostScore = 0;
            for (auto [innerHost, innerFreq] : hostFreqCount) {
                if (innerHost != host) {
                    thisHostScore += innerFreq;
                }
            }

            score += thisHostScore * freq;
        }

        score = int(score / 2);

        return std::make_pair(hostFreqCount.size(), score);
    };

    auto scoreA = getLocalityScore(decisionA);
    auto scoreB = getLocalityScore(decisionB);

    // The first decision is better if it has a LOWER host set size
    if (scoreA.first != scoreB.first) {
        return scoreA.first < scoreB.first;
    }

    // The first decision is better if it has a LOWER locality score
    return scoreA.second < scoreB.second;
}

std::vector<Host> StateAwareScheduler::getSortedHosts(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const DecisionType& decisionType)
{
    std::vector<Host> sortedHosts;
    for (auto [ip, host] : hostMap) {
        sortedHosts.push_back(host);
    }

    std::shared_ptr<SchedulingDecision> oldDecision = nullptr;
    std::map<std::string, int> hostFreqCount;
    if (decisionType != DecisionType::NEW) {
        oldDecision = inFlightReqs.at(req->appid()).second;
        hostFreqCount = getHostFreqCount(oldDecision);
    }

    auto isFirstHostLarger = [&](const Host& hostA, const Host& hostB) -> bool {
        // The BinPack scheduler sorts hosts by number of available slots
        int nAvailableA = numSlotsAvailable(hostA);
        int nAvailableB = numSlotsAvailable(hostB);
        if (nAvailableA != nAvailableB) {
            return nAvailableA > nAvailableB;
        }

        // In case of a tie, it will pick larger hosts first
        int nSlotsA = numSlots(hostA);
        int nSlotsB = numSlots(hostB);
        if (nSlotsA != nSlotsB) {
            return nSlotsA > nSlotsB;
        }

        // Lastly, in case of a tie, return the largest host alphabetically
        return getIp(hostA) > getIp(hostB);
    };

    auto isFirstHostLargerWithFreq = [&](auto hostA, auto hostB) -> bool {
        // When updating an existing scheduling decision (SCALE_CHANGE or
        // DIST_CHANGE), the BinPack scheduler takes into consideration the
        // existing host-message histogram (i.e. how many messages for this app
        // does each host _already_ run)

        int numInHostA = hostFreqCount.contains(getIp(hostA))
                           ? hostFreqCount.at(getIp(hostA))
                           : 0;
        int numInHostB = hostFreqCount.contains(getIp(hostB))
                           ? hostFreqCount.at(getIp(hostB))
                           : 0;

        // If at least one of the hosts has messages for this request, return
        // the host with the more messages for this request (note that it is
        // possible that this host has no available slots at all, in this case
        // we will just pack 0 messages here but we still want to sort it first
        // nontheless)
        if (numInHostA != numInHostB) {
            return numInHostA > numInHostB;
        }

        // In case of a tie, use the same criteria than NEW
        return isFirstHostLarger(hostA, hostB);
    };

    auto isFirstHostLargerWithFreqTaint = [&](const Host& hostA,
                                              const Host& hostB) -> bool {
        // In a DIST_CHANGE decision we want to globally minimise the
        // number of cross-VM links (i.e. best BIN_PACK), but break the ties
        // with hostFreqCount (i.e. if two hosts have the same number of free
        // slots, without counting for the to-be-migrated app, prefer the host
        // that is already running messags for this app)
        int nAvailableA = numSlotsAvailable(hostA);
        int nAvailableB = numSlotsAvailable(hostB);
        if (nAvailableA != nAvailableB) {
            return nAvailableA > nAvailableB;
        }

        // In case of a tie, use the same criteria as FREQ count
        return isFirstHostLargerWithFreq(hostA, hostB);
    };

    switch (decisionType) {
        case DecisionType::NEW: {
            // For a NEW decision type, the BinPack scheduler just sorts the
            // hosts in decreasing order of capacity, and bin-packs messages
            // to hosts in this order
            std::sort(
              sortedHosts.begin(), sortedHosts.end(), isFirstHostLarger);
            break;
        }
        case DecisionType::SCALE_CHANGE: {
            // If we are changing the scale of a running app (i.e. via chaining
            // or thread/process forking) we want to prioritise co-locating
            // as much as possible. This means that we will sort first by the
            // frequency of messages of the running app, and second with the
            // same criteria than NEW
            // IMPORTANT: a SCALE_CHANGE request with 4 messages means that we
            // want to add 4 NEW messages to the running app (not that the new
            // total count is 4)
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      isFirstHostLargerWithFreq);
            break;
        }
        case DecisionType::DIST_CHANGE: {
            // When migrating, we want to know if the provided for app (which
            // is already in-flight) can be improved according to the bin-pack
            // scheduling logic. This is equivalent to saying that the number
            // of cross-vm links can be reduced (i.e. we improve locality)
            auto oldDecision = inFlightReqs.at(req->appid()).second;
            auto hostFreqCount = getHostFreqCount(oldDecision);

            // To decide on a migration opportunity, is like having another
            // shot at re-scheduling the app from scratch. Thus, we remove
            // the current slots we occupy, and return the largest slots.
            // However, in case of a tie, we prefer DIST_CHANGE decisions
            // that minimise the number of migrations, so we need to sort
            // hosts in decreasing order of capacity BUT break ties with
            // frequency
            // WARNING: this assumes negligible migration costs

            // First remove the slots the app occupies to have a fresh new
            // shot at the scheduling
            for (auto h : sortedHosts) {
                if (hostFreqCount.contains(getIp(h))) {
                    freeSlots(h, hostFreqCount.at(getIp(h)));
                }
            }

            // Now sort the emptied hosts breaking ties with the freq count
            // criteria
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      isFirstHostLargerWithFreqTaint);

            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised decision type: {}", decisionType);
            throw std::runtime_error("Unrecognised decision type");
        }
    }

    return sortedHosts;
}

// TODO - USE consistent Hashing !!!
int StateAwareScheduler::getParallelismIndex(const std::string& userFunction,
                                             const faabric::Message& msg)
{
    if (functionParallelism[userFunction] == 1) {
        return 0;
    }
    // For the partitioned stateful. Using key-partitioning.
    if (statePartitionBy.find(userFunction) != statePartitionBy.end()) {
        // get the input KEY.
        std::string inputString = msg.inputdata();
        std::vector<uint8_t> inputVec(inputString.begin(), inputString.end());
        std::map<std::string, std::vector<uint8_t>> inputData =
          faabric::util::deserializeParState(inputVec);
        std::vector<uint8_t> keyData =
          inputData[statePartitionBy[userFunction]];
        SPDLOG_TRACE("UserFunction {}'s input data: {}",
                     userFunction,
                     std::string(keyData.begin(), keyData.end()));
        // Hashing the keyData and set the partition index.
        size_t hashedKey = faabric::util::hashVector(keyData);
        functionCounter[userFunction]++;
        int parallelismIdx = hashedKey % functionParallelism[userFunction];
        SPDLOG_TRACE("UserFunction {}'s hashed key: {} and parallelismIdx {}",
                     userFunction,
                     hashedKey,
                     parallelismIdx);
        return parallelismIdx;
    }
    // Otherwise, use shuffle data-partitioning.
    return functionCounter[userFunction]++ % functionParallelism[userFunction];
}

int getNextMsgId(const std::set<int>& scheduledMessages, int msgIdx)
{
    while (scheduledMessages.find(msgIdx) != scheduledMessages.end()) {
        msgIdx++;
    }
    return msgIdx;
}

bool registerStateToHost(const std::string& userFunctionParIdx,
                         const std::string& host,
                         const std::string& partitionBy,
                         const std::string stateKey)
{
    SPDLOG_DEBUG("Registering state {} to host {}", userFunctionParIdx, host);
    SPDLOG_DEBUG("Partition by: {} and stateKey : {}", partitionBy, stateKey);
    // Update Redis Information
    redis::Redis& redis = redis::Redis::getState();
    std::string mainKey = MAIN_KEY_PREFIX + userFunctionParIdx;
    std::vector<uint8_t> mainIPBytes = faabric::util::stringToBytes(host);
    redis.set(mainKey, mainIPBytes);
    // Send Create information in the host.
    auto [user, function, parallelismId] =
      faabric::util::splitUserFuncPar(userFunctionParIdx);
    state::FunctionStateClient cli(
      user, function, std::stoi(parallelismId), host);
    cli.createState(stateKey);
    return true;
}

// The BinPack's scheduler decision algorithm is very simple. It first sorts
// hosts (i.e. bins) in a specific order (depending on the scheduling type),
// and then starts filling bins from begining to end, until it runs out of
// messages to schedule
std::shared_ptr<SchedulingDecision> StateAwareScheduler::makeSchedulingDecision(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{

    auto decision = std::make_shared<SchedulingDecision>(req->appid(), 0);

    // Get the sorted list of hosts
    auto decisionType = getDecisionType(inFlightReqs, req);
    auto sortedHosts = getSortedHosts(hostMap, inFlightReqs, req, decisionType);

    // Assign slots from the list (i.e. bin-pack)
    auto it = sortedHosts.begin();
    int numLeftToSchedule = req->messages_size();
    std::set<int> scheduledMessages;

    // We want to make sure the decision order is the same as request order.
    // Just to match faasm pattern.
    std::vector<std::string> myDecision(req->messages_size());

    // Before assign slots by using bin-pack. We assign the function-state
    // functions near its function state.
    for (int msgIdx = 0; msgIdx < req->messages_size(); msgIdx++) {
        std::string userFunc =
          req->messages(msgIdx).user() + "_" + req->messages(msgIdx).function();
        // If it is not a function-state function, we don't need to consider it.
        if (!funcStateRegMap.contains(userFunc)) {
            continue;
        }
        // If the function invoke at the first time, register it and create the
        // state.
        if (!functionParallelism.contains(userFunc)) {
            // Default parallelism is 1.
            functionParallelism[userFunc] = 1;
            functionCounter[userFunc] = 0;
            // The default parallelism Id is 0
            std::string funcParaId = userFunc + "_0";
            // TODO - Select a proper host.
            std::string host = sortedHosts[0]->ip;
            stateHost[funcParaId] = host;
            // If the State is partitionedState, register it.
            std::string partitionBy = std::get<0>(funcStateRegMap[userFunc]);
            std::string stateKey = std::get<1>(funcStateRegMap[userFunc]);
            if (partitionBy != "" && stateKey != "") {
                statePartitionBy[userFunc] = partitionBy;
            }
            // Register the state to the host.
            registerStateToHost(funcParaId, host, partitionBy, stateKey);
        }
        // Otherwise, try to assign it to the host that has the function state.
        int parallelismId =
          getParallelismIndex(userFunc, req->messages(msgIdx));
        // Register the parallelismIdx to it. It is safe to register there. Even
        // if the Scheduling failed this time, the next scheduling will
        // overwrite it.
        req->mutable_messages(msgIdx)->set_parallelismid(parallelismId);

        std::string hostKey = userFunc + "_" + std::to_string(parallelismId);
        // If no key is found, ignore it.
        if (stateHost.find(hostKey) == stateHost.end()) {
            continue;
        }
        std::string host = stateHost[hostKey];
        // If host is not available, ignore it.
        if (hostMap.find(host) == hostMap.end()) {
            SPDLOG_WARN(
              "Host {} is not disconnected, but the state in stored in here",
              host);
            continue;
        }
        // If the host has no slot, ignore it.
        if (numSlotsAvailable(hostMap[host]) <= 0) {
            continue;
        }
        myDecision[msgIdx] = getIp(hostMap[host]);
        // decision->addMessage(getIp(hostMap[host]), req->messages(msgIdx));
        scheduledMessages.insert(msgIdx);
        numLeftToSchedule--;
        claimSlots(hostMap[host], 1);
    }

    int msgIdx = getNextMsgId(scheduledMessages, 0);
    // The code for assigning remaining tasks by using bin-pack.
    while (it < sortedHosts.end() && numLeftToSchedule > 0) {
        int numOnThisHost =
          std::min<int>(numLeftToSchedule, numSlotsAvailable(*it));
        for (int i = 0; i < numOnThisHost && msgIdx < req->messages_size();
             i++) {
            // Schedule the message
            myDecision[msgIdx] = getIp(*it);
            // decision->addMessage(getIp(*it), req->messages(msgIdx));
            scheduledMessages.insert(msgIdx);
            numLeftToSchedule--;
            // Get the next available mgsIdx.
            msgIdx++;
            msgIdx = getNextMsgId(scheduledMessages, msgIdx);
            if (msgIdx >= req->messages_size()) {
                // If msgIdx is beyond the range of messages, break out of the
                // loop
                break;
            }
        }

        // If there are no more messages to schedule, we are done
        if (numLeftToSchedule == 0) {
            break;
        }

        // Otherwise, it means that we have exhausted this host, and need to
        // check in the next one
        it++;
    }

    // If we still have enough slots to schedule, we are out of slots
    if (numLeftToSchedule > 0) {
        return std::make_shared<SchedulingDecision>(NOT_ENOUGH_SLOTS_DECISION);
    }

    // Add the messages to the decision
    for (int i = 0; i < myDecision.size(); i++) {
        if (myDecision[i].empty()) {
            throw std::runtime_error(
              "The resource is enough but decision is not fully assigned.");
        }
        decision->addMessage(myDecision[i], req->messages(i));
    }

    // In case of a DIST_CHANGE decision (i.e. migration), we want to make sure
    // that the new decision is better than the previous one
    if (decisionType == DecisionType::DIST_CHANGE) {
        auto oldDecision = inFlightReqs.at(req->appid()).second;
        if (isFirstDecisionBetter(decision, oldDecision)) {
            // If we are sending a better migration, make sure that we minimise
            // the number of migrations to be done
            return minimiseNumOfMigrations(decision, oldDecision);
        }

        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    return decision;
}

bool StateAwareScheduler::registerState(const std::string& userFunction,
                                        const std::string& host,
                                        const std::string& partitionBy)
{
    // The default parallelism is 1
    functionParallelism[userFunction] = 1;
    functionCounter[userFunction] = 0;
    // The default parallelism Id is 0
    std::string funcWithParallelism = userFunction + "_0";
    stateHost[funcWithParallelism] = host;
    if (!partitionBy.empty()) {
        statePartitionBy[userFunction] = partitionBy;
    }
    // Logging functionParallelism
    SPDLOG_TRACE("Function state register has changed, the new state is:");
    for (const auto& pair : functionParallelism) {
        SPDLOG_TRACE("Function: {}, Parallelism: {}", pair.first, pair.second);
    }

    // Logging stateHost
    for (const auto& pair : stateHost) {
        SPDLOG_TRACE("Function: {}, Host: {}", pair.first, pair.second);
    }

    // Logging statePartitionBy
    for (const auto& pair : statePartitionBy) {
        SPDLOG_TRACE(
          "Function: {}, Partitioned By: {}", pair.first, pair.second);
    }

    return true;
}

// TODO - for partitioned state. repartiton the key.
// TODO - change it to increase or decrease function parallelism. It should
// return the old stateHost instead of the true/false
std::shared_ptr<std::map<std::string, std::string>>
StateAwareScheduler::increaseFunctionParallelism(
  const std::string& userFunction,
  HostMap& hostMap)
{
    SPDLOG_DEBUG("Increasing parallelism for {}", userFunction);
    // Double check if the function exists
    if (functionParallelism.find(userFunction) == functionParallelism.end()) {
        SPDLOG_ERROR("Function {} does not exist as function-state function",
                     userFunction);
        return nullptr;
    }
    // Increase the parallelism
    int newParallelismId = functionParallelism[userFunction]++;
    SPDLOG_DEBUG("New parallelism for {} is {}",
                 userFunction,
                 functionParallelism[userFunction]);
    // Construct the stateKey for the new parallelism level
    std::string stateKey =
      userFunction + "_" + std::to_string(newParallelismId);
    // The elements in a std::map are always sorted by key based on a comparison
    // criterion, which is std::less<Key> by default.
    // Now the order to select host is accorrding to the host KEY.
    // Initialize all hosts set to 0 usage
    std::map<std::string, int> usedHosts;
    for (const auto& [ip, host] : hostMap) {
        usedHosts[ip] = 0;
    }
    // Count the used hosts for the specific user function
    for (const auto& [userFuncParallelism, host] : stateHost) {
        if (userFuncParallelism.find(userFunction + "_") != std::string::npos) {
            usedHosts[host]++;
        }
    }
    // Find the host with the minimum count (used the least)
    std::string minHost;
    int minCount = std::numeric_limits<int>::max();
    for (const auto& [host, count] : usedHosts) {
        if (count < minCount) {
            minCount = count;
            minHost = host;
        }
    }
    // Check if a host was found
    if (minHost.empty()) {
        SPDLOG_ERROR("No host found for the new parallelism level");
        return nullptr;
    }
    // Deep copy the stateHost map
    std::map<std::string, std::string> oldStateHost = stateHost;
    auto oldStateHostPtr =
      std::make_shared<std::map<std::string, std::string>>(oldStateHost);
    SPDLOG_DEBUG("Assigning new parallelism {} to {}", stateKey, minHost);
    // Assign the least used host to the new parallelism level
    stateHost[stateKey] = minHost;
    // Update Redis Information
    redis::Redis& redis = redis::Redis::getState();
    std::string mainKey =
      MAIN_KEY_PREFIX + userFunction + "_" + std::to_string(newParallelismId);
    std::vector<uint8_t> mainIPBytes =
      faabric::util::stringToBytes(stateHost[stateKey]);
    redis.set(mainKey, mainIPBytes);
    return oldStateHostPtr;
}

// TODO - for partitioned state. repartiton the key.
// TODO - before repartition. no in flight request.
// TODO - redis state update.
bool StateAwareScheduler::repartitionParitionedState(
  std::string userFunction,
  std::shared_ptr<std::map<std::string, std::string>> oldStateHost)
{
    // Select the new hosts and their parallelismIdx for this function.
    std::shared_ptr<std::map<std::string, std::string>> newFilteredStateHost;
    for (const auto& [userFuncParIdx, host] : stateHost) {
        if (userFuncParIdx.find(userFunction + "_") == std::string::npos) {
            continue;
        }
        newFilteredStateHost->insert({ userFuncParIdx, host });
    }
    // For all the old State Host, notify the new parallelism.
    std::vector<uint8_t> tmpStateHost =
      faabric::util::serializeMapBinary(*newFilteredStateHost);
    std::string newFilteredStateHostStr(tmpStateHost.begin(),
                                        tmpStateHost.end());

    for (const auto& [userFuncParIdx, host] : *oldStateHost) {
        // If the userFuncParallelism is not the userFunction, ignore it.
        if (userFuncParIdx.find(userFunction + "_") == std::string::npos) {
            continue;
        }
        // Inform the new parallelism to the old state server.
        auto [user, function, parallelismId] =
          faabric::util::splitUserFuncPar(userFuncParIdx);
        state::FunctionStateClient cli(
          user, function, std::stoi(parallelismId), host);
        // TODO - for new one, intiliaze new state.
        cli.rePartitionState(newFilteredStateHostStr);
    }

    // Send the new parallelism to state server, along with the new map.
    for (const auto& [userFuncParIdx, host] : *newFilteredStateHost) {
        // Inform the new parallelism to the old state server.
        auto [user, function, parallelismId] =
          faabric::util::splitUserFuncPar(userFuncParIdx);
        state::FunctionStateClient cli(
          user, function, std::stoi(parallelismId), host);
        cli.combineParState();
    }
    // Wait until get the response from all state server.
    return true;
}

void StateAwareScheduler::flushStateInfo()
{
    SPDLOG_DEBUG("Flushing state information");
    functionParallelism.clear();
    functionCounter.clear();
    stateHost.clear();
    statePartitionBy.clear();
}

std::map<std::string, std::map<std::string, int>>
StateAwareScheduler::getAllMetrics(HostMap& hostMap)
{
    std::map<std::string, std::map<std::string, int>> metricsMap;
    // Get the average process time of each function (function and functionPar).

    // Get the congetsion, hold time of each function state.
    return metricsMap;
}

} // namespace faabric::batch_scheduler
