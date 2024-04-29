#pragma once

#include <algorithm>
#include <array>
#include <faabric/util/clock.h>
#include <map>
#include <stddef.h>
#include <string>
#include <numeric>

/* Metrics is used for records the Latency and Throughput for the function.
 * Latency is the process time latency
 * Throughput is the number of function that can be processed in last 1, 10 mins
 * Now This Metrics is designed for the source function (which is invoked by
 * user, not chain_function_call)
 */
namespace faabric::planner {
class FunctionLatency
{
  public:
    FunctionLatency(std::string functionIn);

    // ----------
    // Util public API
    // ----------

    void reset();

    void print();

    std::string getResult();

    // ----------
    // Invoke and Finish public API
    // ----------

    // It is called when a source function is invoked.
    void addInFlightReq(int id);

    // It is called when a source function is finished.
    void removeInFlightReqs(int id);
    // For the functions, we also record the time waiting in batch queue
    void removeInFlightReqs(int id, int batchWaitingTimeIn);

    void updateThroughput(size_t currentMinute);

    // Function name
    std::string function;
    // Number of completed requests from the beginning
    size_t completedRequests;
    size_t averageLatency;
    size_t throughputLastMin;
    size_t throughputLastTenMins;
    size_t waitingQueueCount = 0;
    size_t averageWaitingTime = 0;
    // The map of the function ID to its invoke time
    std::map<int, long> invokeTimeMap;

  private:
    // Variables used for getting the throughput
    // We record the last 10 minues, window size should be 11.
    // now it 15 mins, the LastTen means from 5 to 14, the LastOne means 14
    static const size_t minuteCountsSize = 11;
    std::array<size_t, minuteCountsSize> minuteCounts;
    size_t lastUpdateMinute;
};
}