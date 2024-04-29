#include <faabric/planner/FunctionMetrics.h>

#include <mutex>

namespace faabric::planner {

// std::mutex funcMetricsmtx;

FunctionMetrics::FunctionMetrics(std::string functionIn)
  : function(std::move(functionIn))
{
}
}