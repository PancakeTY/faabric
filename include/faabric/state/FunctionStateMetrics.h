#pragma once

#include <deque>
#include <stdio.h>
#include <faabric/util/queue.h>

namespace faabric::state {
struct FunctionStateMetrics
{
    // Record the latest 100 time that function waiting for aquiring lock.
    util::FixedSizeQueue<int> lockBlockTimeQueue{100};

    // Record the latest 100 time that function holding lock until release.
    util::FixedSizeQueue<int> lockHoldTimeQueue{100};

};

}
