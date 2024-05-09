#ifndef POWER_METER_H
#define POWER_METER_H

#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <linux/perf_event.h>
#include <asm/unistd.h>
#include <string.h>

class PowerMeter {
public:
    PowerMeter();
    ~PowerMeter();

    bool startMeasurement();
    bool stopMeasurement();
    double getEnergyUsage() const;

private:
    int fd;
    long int counter_before;
    long int counter_after;
    struct perf_event_attr attr;
    static constexpr double scale = 2.3283064365386962890625e-10; // from energy-cores.scale

    bool setupCounter();
    bool readCounter(long int &counter);
};

#endif // POWER_METER_H