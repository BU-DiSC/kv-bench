#ifndef EMU_UTIL_H_
#define EMU_UTIL_H_

#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <thread>
#include <cstdio>
#include "sys/times.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/convenience.h"
#include "db/db_impl/db_impl.h"
#include "util/cast_util.h"
#include "emu_environment.h"
#include "workload_stats.h"
#include "aux_time.h"


using namespace rocksdb;


// RocksDB-related helper functions

// Close DB in a way of detecting errors
// followed by deleting the database object when examined to determine if there were any errors. 
// Regardless of errors, it will release all resources and is irreversible.
// Flush the memtable before close 
Status CloseDB(DB *&db, const FlushOptions &flush_op);

// Reopen DB with configured options and a consistent dbptr
// use DB::Close()
Status ReopenDB(DB *&db, const Options &op, const FlushOptions &flush_op);

bool CompactionMayAllComplete(DB *db);
bool FlushMemTableMayAllComplete(DB *db); 
Status BackgroundJobMayAllCompelte(DB *&db);


void printEmulationOutput(const EmuEnv* _env, const QueryTracker *track, uint16_t n = 1);

void configOptions(EmuEnv* _env, Options *op, BlockBasedTableOptions *table_op, WriteOptions *write_op, ReadOptions *read_op, FlushOptions *flush_op);

void populateQueryTracker(QueryTracker *track, DB *_db, const BlockBasedTableOptions& table_options, EmuEnv* _env);

void db_point_lookup(DB* _db, const ReadOptions *read_op, const std::string key, const int verbosity, QueryTracker *query_track);

void write_collected_throughput(std::vector<vector<double> > collected_throughputs, std::vector<std::string> names, std::string path, uint32_t interval);

int runWorkload(DB* _db, const EmuEnv* _env, Options *op,
                const BlockBasedTableOptions *table_op, const WriteOptions *write_op, 
                const ReadOptions *read_op, const FlushOptions *flush_op, EnvOptions* env_op, 
                const WorkloadDescriptor *wd, QueryTracker *query_track,
                std::vector<double >* throughput_collector = nullptr);   // run_workload internal

std::vector<std::string> StringSplit(std::string &str, char delim);


// Print progress bar during workload execution
// n : total number of queries
// count : number of queries finished
// mini_count : keep track of current progress of percentage
inline void showProgress(const uint64_t &n, const uint64_t &count, uint64_t &mini_count) {
  if(count % (n/100) == 0){
  	if (count == n || n == 0) {
	    std::cout << ">OK!\n";
	    return;
  	} 
    if(count % (n/10) == 0) {
      std::cout << ">" << ++mini_count * 10 << "%<";
      fflush(stdout);
    }
  	std::cout << "=";
    fflush(stdout);
  }
}

// Hardcode command to clear system cache 
// May need password to get root access.
inline void clearPageCache() {
  system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
	// sync();
	// std::ofstream ofs("/proc/sys/vm/drop_caches");
	// ofs << "3" << std::endl;
}

// Sleep program for millionseconds
inline void sleep_for_ms(uint32_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

#endif /*EMU_UTIL_H_*/
