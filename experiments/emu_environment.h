/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#ifndef EMU_ENVIRONMENT_H_
#define EMU_ENVIRONMENT_H_

#include <iostream>
#include "rocksdb/table.h"

using namespace std;


class EmuEnv
{
private:
  EmuEnv(); 
  static EmuEnv *instance;

public:
  static EmuEnv* getInstance();

// Options set through command line 
  double size_ratio;                // T | used to set op->max_bytes_for_level_multiplier
  int buffer_size_in_pages;         // P
  int entries_per_page;             // B
  int entry_size;                   // E
  size_t buffer_size;               // M = P*B*E ; in Bytes
  int verbosity;                    // V

  // adding new parameters with Guanting
  uint16_t experiment_runs;               // R
  bool destroy;                           // dd | destroy db before experiments
  bool use_direct_reads;                  // dr
  bool use_direct_io_for_flush_and_compaction; // dw

  // TableOptions
  bool no_block_cache;    // TBC
  int block_cache_capacity; 


  // Other DBOptions
  bool create_if_missing = true;

  // database path 
  string path;
  // workload path
  string ingestion_wpath;
  string query_wpath;
  
  bool show_progress;
  uint32_t eval_point_read_statistics_accuracy_interval;
  // throughput_collect_interval == 0 means not collecting throughput
  uint32_t throughput_collect_interval;
};

#endif /*EMU_ENVIRONMENT_H_*/

