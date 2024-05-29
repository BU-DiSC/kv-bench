#include "workload_stats.h"

void parseIngestionWorkload(WorkloadDescriptor *wd) {
  std::cout << "Parsing ingestion workload ..." << std::endl;
  parseWorkload(wd);
}

void parseQueryWorkload(WorkloadDescriptor *wd) {
  std::cout << "Parsing query workload ..." << std::endl;
  parseWorkload(wd);
}

void parseWorkload(WorkloadDescriptor *wd) {
  assert(wd != nullptr);
  assert(wd->queries.size() == 0);
	std::ifstream f;
	std::string key, start_key, end_key;
	std::string value;
	char mode;
  BaseEntry *tmp = nullptr;
  f.open(wd->path_);
  assert(f);
  while(f >> mode) {
    QueryDescriptor qd;
    switch (mode) {
      case 'I':
        f >> key >> value;
        // create a entry
        tmp = new Entry(key, value);
        qd.seq = wd->queries.size() + 1;
        qd.type = INSERT;
        qd.entry_ptr = tmp;
        wd->queries.push_back(qd);
        ++wd->insert_num;
        ++wd->total_num;
        break;
      case 'U':
        f >> key >> value;
        tmp = new Entry(key, value);
        qd.seq = wd->queries.size() + 1;
        qd.type = UPDATE;
        qd.entry_ptr = tmp; 
        wd->queries.push_back(qd);
        ++wd->update_num;
        ++wd->total_num;
        break;
      case 'D':
        f >> key;
        tmp = new BaseEntry(key);
        qd.seq = wd->queries.size() + 1;
        qd.type = DELETE;
        qd.entry_ptr = tmp; 
        wd->queries.push_back(qd);
        ++wd->pdelete_num;
        ++wd->total_num;
        break;
      case 'R':
        f >> start_key >> end_key;
        tmp = new RangeEntry(start_key, end_key);
        qd.seq = wd->queries.size() + 1;
        qd.type = RANGE_DELETE;
        qd.entry_ptr = tmp; 
        wd->queries.push_back(qd);
        ++wd->rdelete_num;
        ++wd->total_num;
        break;
      case 'Q':
        f >> key;
        tmp = new BaseEntry(key);
        qd.seq = wd->queries.size() + 1;
        qd.type = LOOKUP;
        qd.entry_ptr = tmp; 
        wd->queries.push_back(qd);
        ++wd->plookup_num;
        ++wd->total_num;
        break;
      case 'S':
        f >> start_key >> end_key;
        tmp = new RangeEntry(start_key, end_key);
        qd.seq = wd->queries.size() + 1;
        qd.type = RANGE_LOOKUP;
        qd.entry_ptr = tmp; 
        wd->queries.push_back(qd);
        ++wd->rlookup_num;
        ++wd->total_num;
        break;
      default:
        std::cerr << "Unconfigured query mode: " << mode << std::endl;
        break;
    }
  }
  // for creating pseudo zeor-result point lookup
  wd->actual_insert_num = wd->insert_num;   
  wd->actual_total_num = wd->total_num;    
  f.close();
  std::cout << "Parsing complete ..." << std::endl;
}

void dumpStats(QueryTracker *sample, const QueryTracker *single) {
  if (sample == nullptr) sample = new QueryTracker();
  sample->total_completed += single->total_completed;
  sample->inserts_completed += single->inserts_completed;
  sample->updates_completed += single->updates_completed;
  sample->point_deletes_completed += single->point_deletes_completed;
  sample->range_deletes_completed += single->range_deletes_completed;
  sample->point_lookups_completed += single->point_lookups_completed;
  sample->zero_point_lookups_completed += single->zero_point_lookups_completed;
  sample->range_lookups_completed += single->range_lookups_completed;

  sample->inserts_cost += single->inserts_cost;
  sample->updates_cost += single->updates_cost;
  sample->point_deletes_cost += single->point_deletes_cost;
  sample->range_deletes_cost += single->range_deletes_cost;
  sample->point_lookups_cost += single->point_lookups_cost;
  sample->zero_point_lookups_cost += single->zero_point_lookups_cost;
  sample->range_lookups_cost += single->range_lookups_cost;

  sample->workload_exec_time += single->workload_exec_time;
}
