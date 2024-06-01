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
    maxParallelism = faabric::util::getSystemConfig().maxParallelism;
    // Register the chaining functions in the same topology.
    funcChainedMap["stream_function_source"] = {
        "stream_function_state_source", "stream_function_parstate_source"
    };

    funcChainedMap["stream_wordcount_source"] = { "stream_wordcount_source",
                                                  "stream_wordcount_split",
                                                  "stream_wordcount_count" };

    // Register the function state
    funcStateRegMap["stream_function_state"] = std::make_tuple("", "");
    funcStateRegMap["stream_function_parstate"] =
      std::make_tuple("partitionInputKey", "partitionStateKey");
    funcStateRegMap["stream_wordcount_count"] =
      std::make_tuple("partitionInputKey", "partitionStateKey");

    // Get the preload Infomation
    std::string preloadInfo =
      faabric::util::getSystemConfig().preloadParallelismInfo;

    std::istringstream ss(preloadInfo);
    std::string key;
    int value;

    while (std::getline(ss, key, ',') && ss >> value) {
        preParallelismMap[key] = value;
        // Discard the comma after the value
        if (ss.peek() == ',') {
            ss.ignore();
        }
    }

    // // iterate over the Functions Chained Maps
    // for (const auto [ithSource, ithChainedFunctions] : funcChainedMap) {
    //     // We only increase the parallelism for function-state functions.
    //     for (const auto& function : ithChainedFunctions) {
    //         if (!funcStateRegMap.contains(function)) {
    //             continue;
    //         }
    //         // Update parallelism if necessary.
    //     }
    // }
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
        size_t index = 0;
        std::map<std::string, std::string> inputData =
          faabric::util::deserializeMap(inputVec, index);
        std::string keyData = inputData[statePartitionBy[userFunction]];
        SPDLOG_TRACE("UserFunction {}'s input data: {}", userFunction, keyData);
        // If the HashRing is not initialized, initialize it.
        if (stateHashRing.find(userFunction) == stateHashRing.end()) {
            stateHashRing[userFunction] =
              std::make_shared<faabric::util::ConsistentHashRing>(
                functionParallelism[userFunction]);
        }
        // Hashing the keyData and set the partition index.
        std::vector<uint8_t> keyDataVec = faabric::util::stringToBytes(keyData);
        int parallelismIdx = stateHashRing[userFunction]->getNode(keyDataVec);
        functionCounter[userFunction]++;
        SPDLOG_TRACE(
          "UserFunction {}'s parallelismIdx {}", userFunction, parallelismIdx);
        return parallelismIdx;
    }
    // Otherwise, use shuffle data-partitioning.
    return functionCounter[userFunction]++ % functionParallelism[userFunction];
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

template<typename K, typename V>
K getNthKey(const std::map<K, V>& map, std::size_t n)
{
    if (n >= map.size()) {
        throw std::out_of_range("Index out of range");
    }

    auto it = map.begin();
    std::advance(it, n);
    return it->first;
}

void StateAwareScheduler::initializeState(HostMap& hostMap,
                                          std::string userFunc,
                                          int parallelism)
{
    if (parallelism == 1) {
        // Default parallelism is 1.
        functionParallelism[userFunc] = 1;
        functionCounter[userFunc] = 0;
        // The default parallelism Id is 0
        std::string funcParaId = userFunc + "_0";
        int hostIdx = rbCounter++ % hostMap.size();
        std::string host = getNthKey(hostMap, hostIdx);
        stateHost[funcParaId] = host;
        // If the State is partitionedState, register it.
        std::string partitionBy = std::get<0>(funcStateRegMap[userFunc]);
        std::string stateKey = std::get<1>(funcStateRegMap[userFunc]);
        if (partitionBy != "" && stateKey != "") {
            statePartitionBy[userFunc] = partitionBy;
        }
        // Register the state to the host.
        registerStateToHost(funcParaId, host, partitionBy, stateKey);
    } else {
        // Otherwise initialize it. (We reset all the related stateinfo now)
        functionParallelism[userFunc] = 0;
        functionCounter[userFunc] = 0;
        std::string partitionBy = std::get<0>(funcStateRegMap[userFunc]);
        std::string stateKey = std::get<1>(funcStateRegMap[userFunc]);
        if (partitionBy != "" && stateKey != "") {
            statePartitionBy[userFunc] = partitionBy;
        }
        // Initialize the StateHost and stateHashRing
        increaseFunctionParallelism(parallelism, userFunc, hostMap);
    }
}

std::shared_ptr<SchedulingDecision> StateAwareScheduler::scheduleWithoutLock(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{
    auto decision = std::make_shared<SchedulingDecision>(req->appid(), 0);
    // Round-Robin scheduling
    for (int msgIdx = 0; msgIdx < req->messages_size(); msgIdx++) {
        bool roundRobinFlag = true;
        std::string host = "unknown";

        std::string userFunc =
          req->messages(msgIdx).user() + "_" + req->messages(msgIdx).function();
        // It it is a function-state function, we might need to assign it near
        // the state.
        if (funcStateRegMap.contains(userFunc)) {
            // If function-state has not been initialized, initialize it.
            if (!functionParallelism.contains(userFunc)) {
                initializeState(hostMap, userFunc);
            }
            // TODO - get parallelism is not thread safe now
            int parallelismId =
              getParallelismIndex(userFunc, req->messages(msgIdx));
            // Register the parallelismIdx to it. It is safe to register there.
            // Even if the Scheduling failed this time, the next scheduling will
            // overwrite it.
            req->mutable_messages(msgIdx)->set_parallelismid(parallelismId);
            std::string userFuncPar =
              userFunc + "_" + std::to_string(parallelismId);

            bool unavailableFlag = false;
            // If stateHost doesn't contains the userFuncPar, use round-robin.
            if (stateHost.find(userFuncPar) == stateHost.end()) {
                unavailableFlag = true;
            }
            // If host is not available, use round-robin.
            host = stateHost[userFuncPar];
            if (hostMap.find(host) == hostMap.end()) {
                SPDLOG_WARN("Host {} is not disconnected, but the state in "
                            "stored in here",
                            host);
                unavailableFlag = true;
            }
            if (!unavailableFlag) {
                roundRobinFlag = false;
            }
        }
        // Allocate the request by using round robin.
        if (roundRobinFlag) {
            int hostIdx =
              atomicRbCounter.fetch_add(1, std::memory_order_relaxed) %
              hostMap.size();
            host = getNthKey(hostMap, hostIdx);
        }
        if (host == "unknown") {
            throw std::runtime_error("Host is unknown");
        }
        decision->addMessage(host, req->messages(msgIdx));
    }

    return decision;
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
    int numLeftToSchedule = req->messages_size();

    // Round-Robin scheduling
    for (int msgIdx = 0; msgIdx < req->messages_size(); msgIdx++) {
        bool roundRobinFlag = true;
        std::string host = "unknown";

        std::string userFunc =
          req->messages(msgIdx).user() + "_" + req->messages(msgIdx).function();
        // It it is a function-state function, we might need to assign it near
        // the state.
        if (funcStateRegMap.contains(userFunc)) {
            // If function-state has not been initialized, initialize it.
            if (!functionParallelism.contains(userFunc)) {
                initializeState(hostMap, userFunc);
            }
            int parallelismId =
              getParallelismIndex(userFunc, req->messages(msgIdx));
            // Register the parallelismIdx to it. It is safe to register there.
            // Even if the Scheduling failed this time, the next scheduling will
            // overwrite it.
            req->mutable_messages(msgIdx)->set_parallelismid(parallelismId);
            std::string userFuncPar =
              userFunc + "_" + std::to_string(parallelismId);

            bool unavailableFlag = false;
            // If stateHost doesn't contains the userFuncPar, use round-robin.
            if (stateHost.find(userFuncPar) == stateHost.end()) {
                unavailableFlag = true;
            }
            // If host is not available, use round-robin.
            host = stateHost[userFuncPar];
            if (hostMap.find(host) == hostMap.end()) {
                SPDLOG_WARN("Host {} is not disconnected, but the state in "
                            "stored in here",
                            host);
                unavailableFlag = true;
            }
            // If the host has no slot, use round-robin.
            if (numSlotsAvailable(hostMap[host]) <= 0) {
                unavailableFlag = true;
            }
            if (!unavailableFlag) {
                roundRobinFlag = false;
            }
        }
        // Allocate the request by using round robin.
        if (roundRobinFlag) {
            int hostIdx = rbCounter++ % hostMap.size();
            host = getNthKey(hostMap, hostIdx);
            // TODO - No, We give unlimited slots to each worker.
            if (numSlotsAvailable(hostMap[host]) <= 0) {
                SPDLOG_WARN("Host {} has no available slots", host);
            }
        }
        if (host == "unknown") {
            throw std::runtime_error("Host is unknown");
        }
        decision->addMessage(host, req->messages(msgIdx));
        numLeftToSchedule--;
        claimSlots(hostMap[host], 1);
    }

    // If we still have enough slots to schedule, we are out of slots
    if (numLeftToSchedule > 0) {
        return std::make_shared<SchedulingDecision>(NOT_ENOUGH_SLOTS_DECISION);
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

// TODO - change it to increase or decrease function parallelism. It should
// return the old stateHost instead of the true/false
std::shared_ptr<std::map<std::string, std::string>>
StateAwareScheduler::increaseFunctionParallelism(
  int numIncrease,
  const std::string& userFunction,
  HostMap& hostMap)
{
    SPDLOG_DEBUG("Increasing {} parallelism for {}", numIncrease, userFunction);
    // Double check if the function exists
    if (functionParallelism.find(userFunction) == functionParallelism.end()) {
        SPDLOG_ERROR("Function {} does not exist as function-state function",
                     userFunction);
        return nullptr;
    }
    // Increase the num Parallelism
    int oldPara = functionParallelism[userFunction];
    functionParallelism[userFunction] += numIncrease;
    SPDLOG_DEBUG("New parallelism for {} is {}",
                 userFunction,
                 functionParallelism[userFunction]);
    // Copy the old stateHost to return
    std::map<std::string, std::string> oldStateHost = stateHost;
    auto oldStateHostPtr =
      std::make_shared<std::map<std::string, std::string>>(oldStateHost);
    // Construct the userFunctionIdx for the new parallelism level
    for (size_t idx = oldPara; idx < functionParallelism[userFunction]; idx++) {
        std::string userFunctionIdx = userFunction + "_" + std::to_string(idx);
        // Find the idlest host
        std::map<std::string, int> usedHosts;
        for (const auto& [ip, host] : hostMap) {
            usedHosts[ip] = 0;
        }
        // Count the used hosts for the specific user function
        for (const auto& [userFuncParallelism, host] : stateHost) {
            if (userFuncParallelism.find(userFunction + "_") !=
                std::string::npos) {
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
        SPDLOG_DEBUG(
          "Assigning new parallelism {} to {}", userFunctionIdx, minHost);
        // Assign the least used host to the new parallelism level
        stateHost[userFunctionIdx] = minHost;
        // Update Redis Information and register the state
        std::string partitionBy = std::get<0>(funcStateRegMap[userFunction]);
        std::string stateKey = std::get<1>(funcStateRegMap[userFunction]);
        registerStateToHost(userFunction + "_" + std::to_string(idx),
                            minHost,
                            partitionBy,
                            stateKey);
    }
    if (statePartitionBy.contains(userFunction)) {
        // Change the state hashing ring
        stateHashRing[userFunction] =
          std::make_shared<faabric::util::ConsistentHashRing>(
            functionParallelism[userFunction]);
        // Repartition the state in old stateHost.
        if (oldPara != 0) {
            repartitionParitionedState(userFunction, oldStateHostPtr);
        }
    }
    return oldStateHostPtr;
}

// TODO - before repartition. no in flight request.
bool StateAwareScheduler::repartitionParitionedState(
  std::string userFunction,
  std::shared_ptr<std::map<std::string, std::string>> oldStateHost)
{
    SPDLOG_DEBUG("Repartitioning state for {}", userFunction);
    // Select the new hosts and their parallelismIdx for this function.
    std::map<std::string, std::string> newFilteredStateHost;
    for (const auto& [userFuncParIdx, host] : stateHost) {
        if (userFuncParIdx.find(userFunction + "_") == std::string::npos) {
            continue;
        }
        newFilteredStateHost.insert({ userFuncParIdx, host });
    }
    // For all the old State Host, notify the new parallelism.
    std::vector<uint8_t> tmpStateHost =
      faabric::util::serializeMapBinary(newFilteredStateHost);
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
        cli.rePartitionState(newFilteredStateHostStr);
    }
    // Send the new parallelism to state server, along with the new map.
    for (const auto& [userFuncParIdx, host] : newFilteredStateHost) {
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

void StateAwareScheduler::preloadParallelism(HostMap& hostMap)
{
    if (preParallelismMap.size() == 0) {
        return;
    }

    for (const auto& [ithUserFunction, ithParallelism] : preParallelismMap) {
        // If this function is already initialized, skip it.
        if (functionParallelism.contains(ithUserFunction) &&
            functionParallelism[ithUserFunction] == ithParallelism) {
            continue;
        }
        initializeState(hostMap, ithUserFunction, ithParallelism);
    }

    preParallelismMap.clear();
}

void StateAwareScheduler::updateParallelism(
  HostMap& hostMap,
  std::map<std::string, faabric::planner::FunctionMetrics> metrics)
{
    // Iterate over the Functions Chained Maps
    for (const auto& [ithSource, ithChainedFunctions] : funcChainedMap) {
        // Record the average metrics for each function.
        std::vector<std::string> userFunction;
        std::vector<int> avgLatencyVector;
        std::vector<int> avgLockTimeVector;
        std::vector<int> statefulFunctionIdx;
        int index = 0;

        // We only increase the parallelism for function-state functions.
        for (const auto& function : ithChainedFunctions) {
            // Calculate the average Latency and Locking time for this function.
            // For stateless functions, the parallelism is 1 and it is not
            // recorded.
            int avgLatency = 0;
            int avgLockTime = 0;

            // If this function is stateless
            if (functionParallelism.find(function) ==
                functionParallelism.end()) {
                // Get the average processLatency and lockHoldTime for stateless
                // functions.
                auto metricIt = metrics.find(function + "_0");
                if (metricIt != metrics.end()) {
                    avgLatency = metricIt->second.processLatency;
                    avgLockTime = metricIt->second.lockHoldTime;
                }
            } else {
                // For each parallelism, get the average metrics
                int totalLatency = 0;
                int totalLockTime = 0;
                for (int idx = 0; idx < functionParallelism[function]; idx++) {
                    std::string userFuncPar =
                      function + "_" + std::to_string(idx);
                    auto metricIt = metrics.find(userFuncPar);
                    if (metricIt != metrics.end()) {
                        totalLatency += metricIt->second.processLatency;
                        totalLockTime += metricIt->second.lockHoldTime;
                    }
                }
                // Get the average processLatency and lockHoldTime.
                if (functionParallelism[function] == 0) {
                    SPDLOG_ERROR("Function {} has no parallelism", function);
                } else {
                    avgLatency = totalLatency / functionParallelism[function];
                    avgLockTime = totalLockTime / functionParallelism[function];
                }
            }

            // Record the average metrics for this function.
            userFunction.push_back(function);
            avgLatencyVector.push_back(avgLatency);
            avgLockTimeVector.push_back(avgLockTime);
            if (funcStateRegMap.find(function) != funcStateRegMap.end()) {
                statefulFunctionIdx.push_back(index);
            }
            index++;
        }

        // Increase the stateful function's parallelism if necessary.
        if (statefulFunctionIdx.empty()) {
            continue;
        }

        for (const auto& idx : statefulFunctionIdx) {
            // Calculate the latency except for the current function.
            int totalLatency = 0;
            for (int i = 0; i < avgLatencyVector.size(); i++) {
                if (i == idx) {
                    continue;
                }
                totalLatency += avgLatencyVector[i];
            }
            int incParallelism = 0;
            int freeParallelism =
              maxParallelism - functionParallelism[userFunction[idx]];
            // Increase the parallelism if latency is higher than others.
            if (avgLatencyVector.size() != 1) {
                int avgLatency = totalLatency / (avgLatencyVector.size() - 1);
                if (avgLatency == 0) {
                    incParallelism = freeParallelism;
                } else {
                    incParallelism = std::min(
                      avgLatencyVector[idx] / avgLatency - 1, freeParallelism);
                }
            }
            // TODO - Increase Parallelism if Locking time it High
            if (incParallelism > 0) {
                increaseFunctionParallelism(
                  incParallelism, userFunction[idx], hostMap);
            }
        }
    }
}

void StateAwareScheduler::flushStateInfo()
{
    SPDLOG_DEBUG("Flushing state information");
    functionParallelism.clear();
    functionCounter.clear();
    stateHost.clear();
    statePartitionBy.clear();
}

} // namespace faabric::batch_scheduler
