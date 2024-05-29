#ifndef WORKLOAD_STATS_H_
#define WORKLOAD_STATS_H_

#include <cstdint>
#include <iostream>
#include <fstream>
#include <cmath>
#include <vector>
#include <utility>
#include <unordered_map>
#include <assert.h>

enum QueryType : char {
  INSERT = 'I',
  UPDATE = 'U',
  DELETE = 'D',
  LOOKUP = 'Q',
  RANGE_DELETE = 'R',
  RANGE_LOOKUP = 'S',
  NONE = 0x00
};

// 4 Bytes per point lookup/delete entry
struct BaseEntry{
  std::string key;
  BaseEntry() : key("") {}
  virtual ~BaseEntry() = default;
  explicit BaseEntry(std::string k) : key(k){}
};

// 4 + value size per insert/update query
struct Entry : public BaseEntry{
  std::string value;
  Entry() : BaseEntry(), value("") {}
  explicit Entry(std::string k) : BaseEntry(k), value("") {}
  explicit Entry(std::string k,  std::string v) : BaseEntry(k), value(v) {}
};

// 8 Bytes per range query;
struct RangeEntry : public BaseEntry {
  std::string end_key;
  RangeEntry() : BaseEntry(), end_key("") {}

  explicit RangeEntry(std::string start_key, std::string _end_key)
   : BaseEntry(start_key), end_key(_end_key) {
  }
};

// Store descriptions and apointer to an query entry for efficient use 
// Maximal overhead: 17 Bytes per query;
struct QueryDescriptor {
  uint64_t seq;   // >0 && =index + 1;
  QueryType type;
  BaseEntry *entry_ptr;
  QueryDescriptor() : seq(0), type(NONE), entry_ptr(nullptr) {}
  explicit QueryDescriptor(uint64_t seq_, QueryType ktype, BaseEntry *entry) 
   : seq(seq_), type(ktype), entry_ptr(entry) {}
};

// Store in-memory workload and relative stats 
struct WorkloadDescriptor {
  std::string path_;       // workload path
  uint64_t total_num = 0;
  uint64_t actual_total_num = 0;	
  uint64_t insert_num = 0;
  uint64_t actual_insert_num = 0;		// for pseudo zero result point lookup
  uint64_t update_num = 0;
  uint64_t plookup_num = 0;
  uint64_t rlookup_num = 0;
  uint64_t pdelete_num = 0;
  uint64_t rdelete_num = 0;
  std::vector<QueryDescriptor> queries;
  WorkloadDescriptor() : path_("workload.txt") {}
  explicit WorkloadDescriptor(std::string path) : path_(path) {}
};

// Keep track of all performance metrics during queries execution
struct QueryTracker {
  uint64_t total_completed = 0;
  uint64_t inserts_completed = 0;
  uint64_t updates_completed = 0;
  uint64_t point_deletes_completed = 0;
  uint64_t range_deletes_completed = 0;
  uint64_t point_lookups_completed = 0;
  uint64_t zero_point_lookups_completed = 0;
  uint64_t range_lookups_completed = 0;

  // Cumulative latency cost
  uint64_t inserts_cost = 0;
  uint64_t updates_cost = 0;
  uint64_t point_deletes_cost = 0;
  uint64_t range_deletes_cost = 0;
  uint64_t point_lookups_cost = 0;
  uint64_t zero_point_lookups_cost = 0;
  uint64_t range_lookups_cost = 0;
  uint64_t workload_exec_time = 0;
  uint64_t experiment_exec_time = 0;

};

// Preload workload into memory,
// which is stored in a WorkloadDescriptor
void parseWorkload(WorkloadDescriptor *wd);
void parseIngestionWorkload(WorkloadDescriptor *wd);
void parseQueryWorkload(WorkloadDescriptor *wd);

// Dump stats from a single track into a cumulative sample
// to compute cumulative and average result
void dumpStats(QueryTracker *sample, const QueryTracker *single);

#endif /*WORKLOAD_STATS_H_*/
