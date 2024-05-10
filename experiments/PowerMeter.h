#ifndef POWER_METER_H
#define POWER_METER_H

#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <linux/perf_event.h>
#include <asm/unistd.h>
#include <string.h>

struct EnergyUsage {
    double pkg;
    double ram;
    double core;
};

class PowerMeter {
public:
    PowerMeter();
    ~PowerMeter();

    bool startMeasurement();
    bool stopMeasurement();
    EnergyUsage getEnergyUsage() const;

private:
    int fd_pkg;
    int fd_ram;
    int fd_core;

    long int counter_pkg_before;
    long int counter_pkg_after;
    long int counter_ram_before;
    long int counter_ram_after;
    long int counter_core_before;
    long int counter_core_after;

    struct perf_event_attr attr;
    static constexpr double scale = 2.3283064365386962890625e-10; // scale for all 3 power measurements
    static constexpr int event_cores = 0x1; // event code for cores
    static constexpr int event_pkg = 0x2;   // event code for pkg
    static constexpr int event_ram = 0x3;   // event code for ram
    static constexpr const char* measurement_unit = "Joules";
    static constexpr int type = 0x17; // type is always 23

    bool setupCounter();
    bool readCounter(long int &pkg_counter, long int &ram_counter, long int &core_counter);
};

#endif // POWER_METER_H