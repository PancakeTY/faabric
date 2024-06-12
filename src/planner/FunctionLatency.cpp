#include <faabric/planner/FunctionLatency.h>
#include <faabric/util/clock.h>
#include <faabric/util/logging.h>
#include <iostream>
#include <mutex>

namespace faabric::planner {

std::mutex funcLatencyMtx;

FunctionLatency::FunctionLatency(std::string functionIn)
  : function(std::move(functionIn))
  , completedRequests(0)
  , averageLatency(0)
  , throughputLastMin(0)
  , throughputLastTenMins(0)
  , lastUpdateMinute(0)
{
    std::fill(minuteCounts.begin(), minuteCounts.end(), 0);
}

void FunctionLatency::reset()
{
    std::lock_guard<std::mutex> guard(funcLatencyMtx);
    completedRequests = 0;
    averageLatency = 0;
    throughputLastMin = 0;
    throughputLastTenMins = 0;
    invokeTimeMap.clear();
}

void FunctionLatency::print()
{
    std::lock_guard<std::mutex> guard(funcLatencyMtx);
    auto nowMillis = faabric::util::getGlobalClock().epochMillis();
    // We coarse the time to the minute to save memory.
    size_t currentMinute = nowMillis / 60000; // 60,000 milliseconds in a minute
    updateThroughput(currentMinute);
    std::cout << "Function: " << function << "\n"
              << "Completed Requests: " << completedRequests << "\n"
              << "Average Latency (ms): " << averageLatency << "\n"
              << "Throughput (Last Minute): " << throughputLastMin << "\n"
              << "Throughput (Last 10 Minutes): " << throughputLastTenMins
              << "\n"
              << "Average Waiting Time (ms): " << averageWaitingTime << "\n"
              << "Average Batch Execution Time (ms): "
              << averageExecuteTime << std::endl;
}

std::string FunctionLatency::getResult()
{
    std::lock_guard<std::mutex> guard(funcLatencyMtx);
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

void FunctionLatency::addInFlightReq(int id)
{
    std::lock_guard<std::mutex> guard(funcLatencyMtx);
    long startTime = faabric::util::getGlobalClock().epochMillis();
    invokeTimeMap[id] = startTime;
}

void FunctionLatency::removeInFlightReqs(int id)
{
    std::lock_guard<std::mutex> guard(funcLatencyMtx); // Ensure thread safety
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

void FunctionLatency::removeInFlightReqs(int id,
                                         int batchWaitingTimeIn,
                                         int batchExecutionTimeIn)
{
    {
        std::lock_guard<std::mutex> guard(funcLatencyMtx);
        double batchWaitingTime = static_cast<double>(batchWaitingTimeIn);
        averageWaitingTime =
          (waitingQueueCount > 0)
            ? (averageWaitingTime + (batchWaitingTime - averageWaitingTime) /
                                      (waitingQueueCount + 1.0))
            : batchWaitingTime;
        waitingQueueCount++;
        double batchExecutionTime = static_cast<double>(batchExecutionTimeIn);
        averageExecuteTime =
          (completedRequests > 0)
            ? (averageExecuteTime +
               (batchExecutionTime - averageExecuteTime) /
                 (completedRequests + 1.0))
            : batchExecutionTime;
    }
    removeInFlightReqs(id);
}

void FunctionLatency::removeInFlightReqs(int id,
                                         int plannerQueueTimeIn,
                                         int plannerConsumeTimeIn,
                                         int workerWaitingTimeIn,
                                         int prepareExecuterTimeIn,
                                         int workerExecutionTimeIn,
                                         int totalTimeIn)
{
    {
        std::lock_guard<std::mutex> guard(funcLatencyMtx);

        // Update planner queue time
        double plannerQueueTime = static_cast<double>(plannerQueueTimeIn);
        averagePlannerQueueTime =
            (plannerQueueCount > 0)
                ? (averagePlannerQueueTime +
                   (plannerQueueTime - averagePlannerQueueTime) /
                       (plannerQueueCount + 1.0))
                : plannerQueueTime;
        plannerQueueCount++;

        // Update planner consume time
        double plannerConsumeTime = static_cast<double>(plannerConsumeTimeIn);
        averagePlannerConsumeTime =
            (plannerConsumeCount > 0)
                ? (averagePlannerConsumeTime +
                   (plannerConsumeTime - averagePlannerConsumeTime) /
                       (plannerConsumeCount + 1.0))
                : plannerConsumeTime;
        plannerConsumeCount++;

        // Update worker waiting time
        double workerWaitingTime = static_cast<double>(workerWaitingTimeIn);
        averageWaitingTime =
            (waitingQueueCount > 0)
                ? (averageWaitingTime +
                   (workerWaitingTime - averageWaitingTime) /
                       (waitingQueueCount + 1.0))
                : workerWaitingTime;
        waitingQueueCount++;

        // Update prepare executor time
        double prepareExecuterTime = static_cast<double>(prepareExecuterTimeIn);
        averagePrepareExecuterTime =
            (prepareExecuterCount > 0)
                ? (averagePrepareExecuterTime +
                   (prepareExecuterTime - averagePrepareExecuterTime) /
                       (prepareExecuterCount + 1.0))
                : prepareExecuterTime;
        prepareExecuterCount++;

        // Update worker execution time
        double workerExecutionTime = static_cast<double>(workerExecutionTimeIn);
        averageExecuteTime =
            (completedRequests > 0)
                ? (averageExecuteTime +
                   (workerExecutionTime - averageExecuteTime) /
                       (completedRequests + 1.0))
                : workerExecutionTime;
        completedRequests++;

        // Update total time
        double totalTime = static_cast<double>(totalTimeIn);
        averageTotalTime =
            (totalRequestCount > 0)
                ? (averageTotalTime +
                   (totalTime - averageTotalTime) /
                       (totalRequestCount + 1.0))
                : totalTime;
        totalRequestCount++;
    }
    removeInFlightReqs(id);
}


void FunctionLatency::updateThroughput(size_t currentMinute)
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