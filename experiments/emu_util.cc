#include "emu_util.h"

#include <fstream>
#include <iostream>
#include <iomanip>
#include <queue>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/times.h"

#define WAIT_INTERVAL 100

Status ReopenDB(DB *&db, const Options &op, const FlushOptions &flush_op) {
	const std::string dbPath = db->GetName();
	Status s = CloseDB(db, flush_op);
	assert(s.ok());
	Status return_status = DB::Open(op, dbPath, &db);
	std::cout << return_status.ToString() << std::endl;
	assert(return_status.ok());
	return return_status;
}

Status BackgroundJobMayAllCompelte(DB *&db) {
  Status return_status = Status::Incomplete();
  if (FlushMemTableMayAllComplete(db)) {
    return_status = Status::OK();
  }

	if ((db->GetOptions().disable_auto_compactions == true || 
     (db->GetOptions().disable_auto_compactions == false && CompactionMayAllComplete(db)))) {
		return_status = Status::OK();
	}
 
  return return_status;
}

Status CloseDB(DB *&db, const FlushOptions &flush_op) {
	Status s = db->DropColumnFamily(db->DefaultColumnFamily());
	
	s = db->Flush(flush_op);
	assert(s.ok());
	
  s = BackgroundJobMayAllCompelte(db);
  assert(s.ok());

	s = db->Close();
	assert(s.ok());
	delete db;
	db = nullptr;
	return s;
}

// Need to select timeout carefully
// Completion not guaranteed
bool CompactionMayAllComplete(DB *db) {
	uint64_t pending_compact;
	uint64_t pending_compact_bytes; 
	uint64_t running_compact;
	bool success = db->GetIntProperty("rocksdb.compaction-pending", &pending_compact)
	 						 && db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compact_bytes)
							 && db->GetIntProperty("rocksdb.num-running-compactions", &running_compact);
  do {
    sleep_for_ms(WAIT_INTERVAL);
    success = db->GetIntProperty("rocksdb.compaction-pending", &pending_compact)
	 						 && db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compact_bytes)
							 && db->GetIntProperty("rocksdb.num-running-compactions", &running_compact);

  } while (pending_compact || pending_compact_bytes || running_compact || !success);
	return success;
}

// Need to select timeout carefully
// Completion not guaranteed
bool FlushMemTableMayAllComplete(DB *db) {
	uint64_t pending_flush;
	uint64_t running_flush;
	bool success = db->GetIntProperty("rocksdb.mem-table-flush-pending", &pending_flush)
							 && db->GetIntProperty("rocksdb.num-running-flushes", &running_flush);
  do {
    sleep_for_ms(WAIT_INTERVAL);
    success = db->GetIntProperty("rocksdb.mem-table-flush-pending", &pending_flush)
							 && db->GetIntProperty("rocksdb.num-running-flushes", &running_flush);
  } while (pending_flush || running_flush || !success);
    sleep_for_ms(WAIT_INTERVAL);
	return true;
}

void printEmulationOutput(const EmuEnv* _env, const QueryTracker *track, uint16_t runs) {
  int l = 16;
  std::cout << std::endl;
  std::cout << "-----LSM state-----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "T" << std::setfill(' ') << std::setw(l) 
                                                  << "P" << std::setfill(' ') << std::setw(l) 
                                                  << "B" << std::setfill(' ') << std::setw(l) 
                                                  << "E" << std::setfill(' ') << std::setw(l) 
                                                  << "M" << std::setfill(' ') << std::setw(l);
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << _env->size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size_in_pages;  
  std::cout << std::setfill(' ') << std::setw(l) << _env->entries_per_page;
  std::cout << std::setfill(' ') << std::setw(l) << _env->entry_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size;
  std::cout << std::endl;

  std::cout << std::endl;
  std::cout << "----- Query summary -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "#I" << std::setfill(' ') << std::setw(l)
                                                << "#U" << std::setfill(' ') << std::setw(l)
                                                << "#D" << std::setfill(' ') << std::setw(l)
                                                << "#R" << std::setfill(' ') << std::setw(l)
                                                << "#Q" << std::setfill(' ') << std::setw(l)
                                                << "#Z" << std::setfill(' ') << std::setw(l)
                                                << "#S" << std::setfill(' ') << std::setw(l)
                                                << "#TOTAL" << std::setfill(' ') << std::setw(l);;            
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << track->inserts_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->updates_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->point_deletes_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->range_deletes_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->point_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->zero_point_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->range_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->total_completed/runs;
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "I" << std::setfill(' ') << std::setw(l)
                                                << "U" << std::setfill(' ') << std::setw(l)
                                                << "D" << std::setfill(' ') << std::setw(l)
                                                << "R" << std::setfill(' ') << std::setw(l)
                                                << "Q" << std::setfill(' ') << std::setw(l)
                                                << "Z" << std::setfill(' ') << std::setw(l)
                                                << "S" << std::setfill(' ') << std::setw(l)
                                                << "TOTAL" << std::setfill(' ') << std::setw(l);   

  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->inserts_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->updates_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->point_deletes_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_deletes_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4) 
                                              << static_cast<double>(track->point_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4) 
                                              << static_cast<double>(track->zero_point_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->workload_exec_time)/runs/1000000;
  std::cout << std::endl;

  std::cout << "----- Latency(ms/op) -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "avg_I" << std::setfill(' ') << std::setw(l)
                                                << "avg_U" << std::setfill(' ') << std::setw(l)
                                                << "avg_D" << std::setfill(' ') << std::setw(l)
                                                << "avg_R" << std::setfill(' ') << std::setw(l)
                                                << "avg_Q" << std::setfill(' ') << std::setw(l)
                                                << "avg_Z" << std::setfill(' ') << std::setw(l)
                                                << "avg_S" << std::setfill(' ') << std::setw(l)
                                                << "avg_query" << std::setfill(' ') << std::setw(l);   

  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->inserts_cost) / track->inserts_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->updates_cost) / track->updates_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->point_deletes_cost) / track->point_deletes_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_deletes_cost) / track->range_deletes_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4) 
                                              << static_cast<double>(track->point_lookups_cost) / track->point_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4) 
                                              << static_cast<double>(track->zero_point_lookups_cost) / track->zero_point_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_lookups_cost) / track->range_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->workload_exec_time) / track->total_completed / 1000000;
  std::cout << std::endl;

  std::cout << "----- Throughput(op/ms) -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "avg_I" << std::setfill(' ') << std::setw(l)
            << "avg_U" << std::setfill(' ') << std::setw(l)
            << "avg_D" << std::setfill(' ') << std::setw(l)
            << "avg_R" << std::setfill(' ') << std::setw(l)
            << "avg_Q" << std::setfill(' ') << std::setw(l)
            << "avg_Z" << std::setfill(' ') << std::setw(l)
            << "avg_S" << std::setfill(' ') << std::setw(l)
            << "avg_query" << std::setfill(' ') << std::setw(l);

  std::cout << std::endl;

  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->inserts_completed*1000000) / track->inserts_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->updates_completed*1000000) / track->updates_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->point_deletes_completed*1000000) / track->point_deletes_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->range_deletes_completed*1000000) / track->range_deletes_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4)
            << static_cast<double>(track->point_lookups_completed*1000000) / track->point_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4)
            << static_cast<double>(track->zero_point_lookups_completed*1000000) / track->zero_point_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->range_lookups_completed*1000000) / track->range_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->total_completed*1000000) / track->workload_exec_time;
  std::cout << std::endl; 
}

void configOptions(EmuEnv* _env, Options *op, BlockBasedTableOptions *table_op, WriteOptions *write_op, ReadOptions *read_op, FlushOptions *flush_op) {
    // Experiment settings
    _env->experiment_runs = (_env->experiment_runs >= 1) ? _env->experiment_runs : 1;

    // *op = Options();
    op->write_buffer_size = _env->buffer_size; 

    op->max_bytes_for_level_multiplier = _env->size_ratio; 
    op->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio;
 
  //Other DBOptions
  op->create_if_missing = _env->create_if_missing;
  op->use_direct_reads = _env->use_direct_reads;
  op->use_direct_io_for_flush_and_compaction = _env->use_direct_io_for_flush_and_compaction;

  //TableOptions
  table_op->no_block_cache = _env->no_block_cache; // TBC
  if(table_op->no_block_cache){
     _env->block_cache_capacity = 0;
  }
  if (_env->block_cache_capacity == 0) {
      ;// do nothing
  } else {
      std::shared_ptr<Cache> cache = NewLRUCache(_env->block_cache_capacity*1024, -1, false, 0.5);
      table_op->block_cache = cache;
  }
   
  table_op->block_size = _env->entries_per_page * _env->entry_size;
   
  table_op->filter_policy.reset(NewBloomFilterPolicy(8.0));
  // Set all table options
  op->table_factory.reset(NewBlockBasedTableFactory(*table_op)); 
}

void populateQueryTracker(QueryTracker *query_track, DB* _db, const BlockBasedTableOptions& table_options, EmuEnv* _env) {
  query_track->workload_exec_time = query_track->inserts_cost + query_track->updates_cost + query_track->point_deletes_cost 
                                    + query_track->range_deletes_cost + query_track->point_lookups_cost + query_track->zero_point_lookups_cost
                                    + query_track->range_lookups_cost;
}

void db_point_lookup(DB* _db, const ReadOptions *read_op, const std::string key, const int verbosity, QueryTracker *query_track){
    my_clock start_clock, end_clock;    // clock to get query time
    string value;
    Status s;

    if (verbosity == 2)
      std::cout << "Q " << key << "" << std::endl;
    if (my_clock_get_time(&start_clock) == -1)
      std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
    //s = db->Get(*read_op, ToString(key), &value);
    s = _db->Get(*read_op, key, &value);
    // assert(s.ok());
    if (my_clock_get_time(&end_clock) == -1) 
      std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
    
    if (!s.ok()) {    // zero_reuslt_point_lookup
        if (verbosity == 2) {

           std::cerr << s.ToString() << "key = " << key << std::endl;
        }
        query_track->zero_point_lookups_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->zero_point_lookups_completed;
    } else{
        query_track->point_lookups_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->point_lookups_completed;
    }
    ++query_track->total_completed;
}

void write_collected_throughput(std::vector<vector<double> > collected_throughputs, std::vector<std::string> names, std::string throughput_path, uint32_t interval) {
  assert(collected_throughputs.size() == names.size());
  assert(collected_throughputs.size() > 0);
  ofstream throughput_ofs(throughput_path.c_str());
  throughput_ofs << "ops";
  for (int i = 0; i < names.size(); i++) {
    throughput_ofs << ",tput-" << names[i];
  }
  throughput_ofs << std::endl;
  for (int j = 0; j < collected_throughputs[0].size(); j++) {
    throughput_ofs << j*interval;
    for (int i = 0; i < collected_throughputs.size(); i++) {
      throughput_ofs << "," << collected_throughputs[i][j];
    }
    throughput_ofs << std::endl;
  }
  throughput_ofs.close();
}

// Run a workload from memory
// The workload is stored in WorkloadDescriptor
// Use QueryTracker to record performance for each query operation
int runWorkload(DB* _db, const EmuEnv* _env, Options *op, const BlockBasedTableOptions *table_op, 
                const WriteOptions *write_op, const ReadOptions *read_op, const FlushOptions *flush_op,
                EnvOptions* env_op, const WorkloadDescriptor *wd, QueryTracker *query_track,
                std::vector<double >* throughput_collector) {
  Status s;
  Iterator *it = _db-> NewIterator(*read_op); // for range reads
  uint64_t counter = 0, mini_counter = 0; // tracker for progress bar. TODO: avoid using these two 
  
  bool collect_throughput_flag = false;
  if (throughput_collector != nullptr) {
    collect_throughput_flag = true;
    throughput_collector->clear();
  }
  my_clock start_clock, end_clock;    // clock to get query time

  uint64_t total_ingestion_num = wd->insert_num + wd->update_num + wd->pdelete_num + wd->rdelete_num; 
  for (const auto &qd : wd->queries) { 
    std::string key, start_key, end_key;
    std::string value;
    int thread_index;
    Entry *entry_ptr = nullptr;
    RangeEntry *rentry_ptr = nullptr;

    switch (qd.type) {
      case INSERT:
        ++counter;
        assert(counter = qd.seq);
        if (query_track->inserts_completed == wd->actual_insert_num) break;
	
        entry_ptr = static_cast<Entry*>(qd.entry_ptr);
        key = entry_ptr->key;
        value = entry_ptr->value;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << " " << value << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = _db->Put(*write_op, key, value);
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        assert(s.ok());
        query_track->inserts_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->total_completed;
        ++query_track->inserts_completed;
        break;

      case UPDATE:
        ++counter;
        assert(counter = qd.seq);
        entry_ptr = static_cast<Entry*>(qd.entry_ptr);
        key = entry_ptr->key;
        value = entry_ptr->value;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << " " << value << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = _db->Put(*write_op, key, value);
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->updates_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->updates_completed;
        ++query_track->total_completed;
        break;

      case DELETE:
        ++counter;
        assert(counter = qd.seq);
        key = qd.entry_ptr->key;	
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = _db->Delete(*write_op, key);
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->point_deletes_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->point_deletes_completed;
        ++query_track->total_completed;
        break;

      case RANGE_DELETE:
        ++counter;
        assert(counter = qd.seq);	
        rentry_ptr = static_cast<RangeEntry*>(qd.entry_ptr);
        start_key = rentry_ptr->key;
        end_key = rentry_ptr->end_key;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << start_key << " " << end_key << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = _db->DeleteRange(*write_op, _db->DefaultColumnFamily(), start_key, end_key);
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->range_deletes_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->range_deletes_completed;
        ++query_track->total_completed;
        break;

      case LOOKUP:
        ++counter;
        assert(counter = qd.seq);
        key = qd.entry_ptr->key; 
        db_point_lookup(_db, read_op, key, _env->verbosity, query_track);
        break;

      case RANGE_LOOKUP:
        ++counter; 
        assert(counter = qd.seq);
        rentry_ptr = static_cast<RangeEntry*>(qd.entry_ptr);
        start_key = rentry_ptr->key;
        //end_key = start_key + rentry_ptr->range;
        end_key = rentry_ptr->end_key;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << start_key << " " << end_key << "" << std::endl;
        it->Refresh();    // to update a stale iterator view
        assert(it->status().ok());
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
       
        for (it->Seek(start_key); it->Valid(); it->Next()) {
          if(it->key() == end_key) {    // TODO: check correntness
            break;
          }
        }
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        if (!it->status().ok()) {
          std::cerr << it->status().ToString() << std::endl;
        }
        query_track->range_lookups_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->range_lookups_completed;
        ++query_track->total_completed;
        break;

      default:
          std::cerr << "Unknown query type: " << static_cast<char>(qd.type) << std::endl;
    }
     
    showProgress(wd->total_num, counter, mini_counter);

    if (collect_throughput_flag) {
      if (counter%_env->throughput_collect_interval == 0) {
         uint64_t exec_time = query_track->inserts_cost + query_track->updates_cost + query_track->point_deletes_cost 
                                    + query_track->range_deletes_cost + query_track->point_lookups_cost + query_track->zero_point_lookups_cost
                                    + query_track->range_lookups_cost;
        if (counter != 0 && exec_time != 0) {
          throughput_collector->push_back(counter*1.0/exec_time);
        } else {
          throughput_collector->push_back(0.0);
        }
      }  
    } 
  }

  if (collect_throughput_flag) {
    uint64_t exec_time = query_track->inserts_cost + query_track->updates_cost + query_track->point_deletes_cost 
                                    + query_track->range_deletes_cost + query_track->point_lookups_cost + query_track->zero_point_lookups_cost
                                    + query_track->range_lookups_cost;
        if (counter != 0 && exec_time != 0) {
          throughput_collector->push_back(counter*1.0/exec_time);
        } else {
          throughput_collector->push_back(0.0);
        }
  }


  delete it; 
  it = nullptr;

  // flush and wait for the steady state
  _db->Flush(*flush_op);
  return 0;
}

std::vector<std::string> StringSplit(std::string &str, char delim){
  std::vector<std::string> splits;
  std::stringstream ss(str);
  std::string item;
  while(getline(ss, item, delim)){
    splits.push_back(item);
  }
  return splits;
}
