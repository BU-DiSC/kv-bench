#include "PowerMeter.h"
#include <stdio.h>
#include <stdlib.h>

#define PERF_EVENT_OPEN_SYSCALL_NR __NR_perf_event_open

PowerMeter::PowerMeter() : fd(-1), counter_before(0), counter_after(0) {
    memset(&attr, 0, sizeof(attr));
    attr.type = 0x17; // 23 in hex
    attr.size = sizeof(struct perf_event_attr);
    attr.config = 0x02; // event code in hex
}

PowerMeter::~PowerMeter() {
    if (fd != -1) {
        close(fd);
    }
}

bool PowerMeter::setupCounter() {
    fd = syscall(PERF_EVENT_OPEN_SYSCALL_NR, &attr, -1, 0, -1, 0);
    return fd != -1;
}

bool PowerMeter::startMeasurement() {
    if (!setupCounter()) {
        perror("perf_event_open");
        return false;
    }
    return readCounter(counter_before);
}

bool PowerMeter::stopMeasurement() {
    if (!readCounter(counter_after)) {
        return false;
    }
    close(fd);
    fd = -1;
    return true;
}

bool PowerMeter::readCounter(long int &counter) {
    if (read(fd, &counter, sizeof(long int)) != sizeof(long int)) {
        perror("read");
        close(fd);
        fd = -1;
        return false;
    }
    return true;
}

double PowerMeter::getEnergyUsage() const {
    return (counter_after - counter_before) * scale;
}