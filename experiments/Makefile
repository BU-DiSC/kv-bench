rocksdbpath=../rocksdb
include ${rocksdbpath}/make_config.mk


ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti -g
endif

.PHONY: clean librocksdb

all: plain_benchmark_metrics

plain_benchmark_metrics: librocksdb emu_environment.cc workload_stats.cc aux_time.cc emu_util.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ emu_environment.cc emu_util.cc workload_stats.cc aux_time.cc ${rocksdbpath}/librocksdb.a -I${rocksdbpath}/include -I${rocksdbpath} -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf plain_benchmark plain_benchmark_debug

librocksdb:
	cd ${rocksdbpath} && $(MAKE) static_lib

librocksdb_debug:
	cd ${rocksdbpath} && $(MAKE) dbg