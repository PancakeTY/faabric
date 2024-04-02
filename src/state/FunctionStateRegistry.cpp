#include <faabric/redis/Redis.h>
#include <faabric/state/FunctionState.h>
#include <faabric/state/FunctionStateRegistry.h>
#include <faabric/util/bytes.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/state.h>

#include <vector>

#define MAIN_KEY_PREFIX "main_"

namespace faabric::state {
FunctionStateRegistry& getFunctionStateRegistry()
{
    static FunctionStateRegistry reg;
    return reg;
}

static std::string getMasterKey(const std::string& user,
                                const std::string& func,
                                size_t parallelismId)
{
    std::string mainKey =
      MAIN_KEY_PREFIX + user + "_" + func + "_" + std::to_string(parallelismId);
    return mainKey;
}

std::string FunctionStateRegistry::getMasterIP(const std::string& user,
                                               const std::string& func,
                                               int parallelismId,
                                               const std::string& hotsIP,
                                               bool claim)
{
    std::string lookupKey =
      faabric::util::keyForFunction(user, func, parallelismId);

    // See if we already have the main
    {
        faabric::util::SharedLock lock(mainMapMutex);
        if (mainMap.count(lookupKey) > 0) {
            return mainMap[lookupKey];
        }
    }

    // No main found, need to establish

    // Acquire lock
    faabric::util::FullLock lock(mainMapMutex);

    // Double check condition
    if (mainMap.count(lookupKey) > 0) {
        return mainMap[lookupKey];
    }

    SPDLOG_TRACE("Checking main for state {}", lookupKey);

    // Query Redis
    const std::string mainKey = getMasterKey(user, func, parallelismId);
    redis::Redis& redis = redis::Redis::getState();
    std::vector<uint8_t> mainIPBytes = redis.get(mainKey);

    if (mainIPBytes.empty() && !claim) {
        // No main found and not claiming
        SPDLOG_TRACE("No main found for {}", lookupKey);
        throw FunctionStateException("Found no main for state " + mainKey);
    }

    // If no main and we want to claim, attempt to do so
    if (mainIPBytes.empty()) {
        uint32_t mainLockId = FunctionState::waitOnRedisRemoteLockFunc(mainKey);
        if (mainLockId == 0) {
            SPDLOG_ERROR("Unable to acquire remote lock for {}", mainKey);
            throw std::runtime_error("Unable to get remote lock");
        }

        SPDLOG_DEBUG("Claiming main for {} (this host {})", lookupKey, hotsIP);

        // Check there's still no main, if so, claim
        mainIPBytes = redis.get(mainKey);
        if (mainIPBytes.empty()) {
            mainIPBytes = faabric::util::stringToBytes(hotsIP);
            redis.set(mainKey, mainIPBytes);
        }

        redis.releaseLock(mainKey, mainLockId);
    }

    // Cache the result locally
    std::string mainIP = faabric::util::bytesToString(mainIPBytes);
    SPDLOG_DEBUG(
      "Caching main for {} as {} (this host {})", lookupKey, mainIP, hotsIP);

    mainMap[lookupKey] = mainIP;

    return mainIP;
}

std::string FunctionStateRegistry::getMasterIPForOtherMaster(
  const std::string& userIn,
  const std::string& funcIn,
  int parallelismIdIn,
  const std::string& hotsIP)
{
    // Get the main IP
    std::string mainIP =
      getMasterIP(userIn, funcIn, parallelismIdIn, hotsIP, false);

    // Sanity check that the main is *not* this machine
    if (mainIP == hotsIP) {
        SPDLOG_ERROR("Attempting to pull state size on main ({}/{}-{} on {})",
                     userIn,
                     funcIn,
                     parallelismIdIn,
                     hotsIP);
        throw std::runtime_error("Attempting to pull state size on main");
    }

    return mainIP;
}

void FunctionStateRegistry::clear()
{
    faabric::util::FullLock lock(mainMapMutex);
    mainMap.clear();
}

}
