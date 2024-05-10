#include "PowerMeter.h"
#include <stdio.h>
#include <stdlib.h>

#define PERF_EVENT_OPEN_SYSCALL_NR __NR_perf_event_open

struct perf_event_attr pkg_attr, ram_attr, core_attr;

PowerMeter::PowerMeter() : fd_pkg(-1), fd_ram(-1), fd_core(-1), counter_pkg_before(0), counter_pkg_after(0), counter_ram_before(0), counter_ram_after(0), counter_core_before(0), counter_core_after(0) {
    memset(&pkg_attr, 0, sizeof(pkg_attr));
    memset(&ram_attr, 0, sizeof(ram_attr));
    memset(&core_attr, 0, sizeof(core_attr));

    // Setup attributes for each counter
    pkg_attr.type = type;
    pkg_attr.size = sizeof(struct perf_event_attr);
    pkg_attr.config = event_pkg;

    ram_attr.type = type;
    ram_attr.size = sizeof(struct perf_event_attr);
    ram_attr.config = event_ram;

    core_attr.type = type;
    core_attr.size = sizeof(struct perf_event_attr);
    core_attr.config = event_cores;
}

PowerMeter::~PowerMeter() {
    if (fd_pkg != -1) close(fd_pkg);
    if (fd_ram != -1) close(fd_ram);
    if (fd_core != -1) close(fd_core);
}

bool PowerMeter::setupCounter() {
    fd_pkg = syscall(PERF_EVENT_OPEN_SYSCALL_NR, &pkg_attr, -1, 0, -1, 0);
    fd_ram = syscall(PERF_EVENT_OPEN_SYSCALL_NR, &ram_attr, -1, 0, -1, 0);
    fd_core = syscall(PERF_EVENT_OPEN_SYSCALL_NR, &core_attr, -1, 0, -1, 0);
    return (fd_pkg != -1 && fd_ram != -1 && fd_core != -1);
}

bool PowerMeter::startMeasurement() {
    if (!setupCounter()) {
        perror("PowerMeter::startMeasurement");
        return false;
    }
    return readCounter(counter_pkg_before, counter_ram_before, counter_core_before);
}

bool PowerMeter::stopMeasurement() {
    if (!readCounter(counter_pkg_after, counter_ram_after, counter_core_after)) {
        return false;
    }
    close(fd_pkg); fd_pkg = -1;
    close(fd_ram); fd_ram = -1;
    close(fd_core); fd_core = -1;
    return true;
}

bool PowerMeter::readCounter(long int &pkg_counter, long int &ram_counter, long int &core_counter) {
    if (read(fd_pkg, &pkg_counter, sizeof(long int)) != sizeof(long int) ||
        read(fd_ram, &ram_counter, sizeof(long int)) != sizeof(long int) ||
        read(fd_core, &core_counter, sizeof(long int)) != sizeof(long int)) {
        perror("PowerMeter::readCounter");
        close(fd_pkg); fd_pkg = -1;
        close(fd_ram); fd_ram = -1;
        close(fd_core); fd_core = -1;
        return false;
    }
    return true;
}

EnergyUsage PowerMeter::getEnergyUsage() const {
    EnergyUsage usage;
    usage.pkg = (counter_pkg_after - counter_pkg_before) * scale;
    usage.ram = (counter_ram_after - counter_ram_before) * scale;
    usage.core = (counter_core_after - counter_core_before) * scale;
    return usage;
}