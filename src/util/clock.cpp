#include <faabric/util/clock.h>

namespace faabric::util {
Clock& getGlobalClock()
{
    static Clock instance;
    return instance;
}

Clock::Clock() = default;

const TimePoint Clock::now()
{
    return std::chrono::steady_clock::now();
}

const long Clock::epochMillis()
{
    long millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

    return millis;
}

const long long Clock::epochMicros()
{
    // Get the current time_point from the high_resolution_clock
    auto now = std::chrono::high_resolution_clock::now();

    // Convert the time_point to a duration in microseconds since epoch
    auto duration = now.time_since_epoch();

    // Convert the duration to microseconds
    long long microseconds =
      std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

    return microseconds;
}

const long long Clock::epochNanos()
{
    // Get the current time_point from the high_resolution_clock
    auto now = std::chrono::high_resolution_clock::now();

    // Convert the time_point to a duration in nanoseconds since epoch
    auto duration = now.time_since_epoch();

    // Convert the duration to nanoseconds
    long long nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

    return nanoseconds;
}

const long Clock::timeDiff(const TimePoint& t1, const TimePoint& t2)
{
    long age =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t2).count();
    return age;
}

const long Clock::timeDiffMicro(const TimePoint& t1, const TimePoint& t2)
{
    long age =
      std::chrono::duration_cast<std::chrono::microseconds>(t1 - t2).count();
    return age;
}

const long Clock::timeDiffNano(const TimePoint& t1, const TimePoint& t2)
{
    long age =
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t2).count();
    return age;
}
}
