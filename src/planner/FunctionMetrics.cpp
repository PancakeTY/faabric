#include <faabric/planner/FunctionMetrics.h>
#include <faabric/util/clock.h>
#include <faabric/util/logging.h>
#include <iostream>
#include <mutex>

namespace faabric::planner {

std::mutex mtx;

FunctionMetrics::FunctionMetrics(std::string functionIn)
  : function(std::move(functionIn))
  , completedRequests(0)
  , averageLatency(0)
  , throughputLastMin(0)
  , throughputLastTenMins(0)
  , lastUpdateMinute(0)
{
    std::fill(minuteCounts.begin(), minuteCounts.end(), 0);
}

void FunctionMetrics::reset()
{
    std::lock_guard<std::mutex> guard(mtx);
    completedRequests = 0;
    averageLatency = 0;
    throughputLastMin = 0;
    throughputLastTenMins = 0;
    invokeTimeMap.clear();
}

void FunctionMetrics::print()
{
    std::lock_guard<std::mutex> guard(mtx);
    auto nowMillis = faabric::util::getGlobalClock().epochMillis();
    // We coarse the time to the minute to save memory.
    size_t currentMinute = nowMillis / 60000; // 60,000 milliseconds in a minute
    updateThroughput(currentMinute);
    std::cout << "Function: " << function << "\n"
              << "Completed Requests: " << completedRequests << "\n"
              << "Average Latency (ms): " << averageLatency << "\n"
              << "Throughput (Last Minute): " << throughputLastMin << "\n"
              << "Throughput (Last 10 Minutes): " << throughputLastTenMins
              << std::endl;
}

std::string FunctionMetrics::getResult()
{
    std::lock_guard<std::mutex> guard(mtx);
    auto nowMillis = faabric::util::getGlobalClock().epochMillis();
    // We coarse the time to the minute to save memory.
    size_t currentMinute = nowMillis / 60000; // 60,000 milliseconds in a minute
    updateThroughput(currentMinute);
    return "Function: " + function +
           ", Completed Requests: " + std::to_string(completedRequests) +
           ", Average Latency (ms): " + std::to_string(averageLatency) +
           ", Throughput (Last Minute): " + std::to_string(throughputLastMin) +
           ", Throughput (Last 10 Minutes): " +
           std::to_string(throughputLastTenMins);
}

void FunctionMetrics::addInFlightReq(int id)
{
    std::lock_guard<std::mutex> guard(mtx);
    long startTime = faabric::util::getGlobalClock().epochMillis();
    invokeTimeMap[id] = startTime;
}

void FunctionMetrics::removeInFlightReqs(int id)
{
    std::lock_guard<std::mutex> guard(mtx); // Ensure thread safety
    auto it = invokeTimeMap.find(id);
    if (it == invokeTimeMap.end()) {
        SPDLOG_WARN(
          "{} Could not find start time for request {}", function, id);
        return;
    }

    long startTime = it->second;
    long endTime = faabric::util::getGlobalClock().epochMillis();
    double requestLatency = static_cast<double>(endTime - startTime);

    // Update the running average for latency using the provided formula
    averageLatency = (completedRequests > 0)
                       ? (averageLatency + (requestLatency - averageLatency) /
                                             (completedRequests + 1.0))
                       : requestLatency;

    completedRequests++;
    invokeTimeMap.erase(it);

    auto nowMillis = faabric::util::getGlobalClock().epochMillis();
    // We coarse the time to the minute to save memory.
    size_t currentMinute = nowMillis / 60000; // 60,000 milliseconds in a minute
    updateThroughput(currentMinute);
    minuteCounts[currentMinute % minuteCounts.size()]++;
}

void FunctionMetrics::updateThroughput(size_t currentMinute)
{
    size_t minutesElapsed = currentMinute - lastUpdateMinute;

    // If the minutes elapsed is greater than 10, clear the queue.
    if (minutesElapsed >= minuteCountsSize) {
        std::fill(minuteCounts.begin(), minuteCounts.end(), 0);
    } else {
        for (size_t i = 1; i <= minutesElapsed; ++i) {
            size_t minuteIndex = (lastUpdateMinute + i) % minuteCountsSize;
            minuteCounts[minuteIndex] = 0; // Reset counts for "passed" minutes
        }
    }

    lastUpdateMinute = currentMinute;

    // The throughput for the last minute is now the count from the previous
    // minute
    size_t lastMinIndex = (currentMinute - 1) % minuteCountsSize;
    throughputLastMin = minuteCounts[lastMinIndex];

    // Calculate throughput for the last ten minutes, excluding the current and
    // the previous minute
    throughputLastTenMins =
      std::accumulate(minuteCounts.begin(), minuteCounts.end(), 0) -
      minuteCounts[currentMinute % minuteCountsSize];
}

} // namespace faabric::planner