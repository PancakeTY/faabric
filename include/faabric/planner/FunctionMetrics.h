#pragma once

#include <string>

namespace faabric::planner {
class FunctionMetrics
{
  public:
    FunctionMetrics(std::string functionIn);

    // There are two tpyes of Function Metrics
    // Metrics for a single function: user_func_par
    // Metrics for a chained functions (topology): user_func (the source func)
    std::string function;
    // chained means this metrics is statistics of chained functions not only
    // one function
    bool isChained = false;
    // Throughput Last miniute
    int throughput = 0;
    int waitingTime = 0;
    int processLatency = 0;
    int lockCongestionTime = 0;
    int lockHoldTime = 0;
    int averageWaitingTime = 0;
    int averageExecuteTime = 0;

  private:
};
}