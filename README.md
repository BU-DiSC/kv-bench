<H1> The codebase for "KVBench: A Key-Value Benchmarking Suite" </H1>

This repository contains three submodules: key-value workload generator, RocksDB (v8.9.1), and an example code to benchmark RocksDB using our workload.

<H1> Workload Generation </H1>

To generate a key-value workload, you need to go into directory `K-V-Workload-Generator` and simply give 

```
make
./load_gen -I10000
```

with the desired parameters. These include: Number of inserts, updates, deletes, point & range lookups, distribution styles, etc. 
In the above example, it will generate a workload file (e.g., `workload.txt`) with 10000 inserts (for pre-populating a database).

We can further use preloading feature to generate another workload to benchmark. For example, we can run

```
./load_gen --PL -Q3000 --OP query_workload.txt
```
This will generate a text file `query_workload.txt` that contains 3000 point queries on existing keys by preloading `workload.txt` generated earlier.

To vary the distribution of point queries, you can specify `--ED [ED] --ZD [ZD]`, where `[ED]` and `[ZD]` represent the distribution number for existing and non-existing point queries, respectively (distribution number: 0->uniform, 1->normal, 2->beta, 3->zipf)

More details can be found by running `./load_gen --help`.

<H1> Compiling RocksDB </H1>

After that, make sure that you have also compiled RocksDB. To do that under `rocksdb` directory and run:
```
make static_lib
```
If you cannot compile the static library successfully due to lack of package, please check [here](https://github.com/facebook/rocksdb/blob/main/INSTALL.md) for more info.

<H1> Running the Example Benchmark for RocksDB </H1>

To run the workload, you need to go to `kv-bench-rocksdb-example` directory and just give:

```
make
./plain_benchmark -E [E] --dd --iwp [path/to/ingestion_workload] --qwp [path/to/benchmark_workload] --dw --dr
```

where `[E]` is the entry size generated from earlier workload generator, `[path/to/ingestion_workload]` means the path of the ingestion workload to pre-populate a database and `[path/to/benchmark_workload]` means the path of benchmark.

If you follow the earlier instructions to generate the workload (by default, the entry size is 8 if you do not specify anything else), we can replace the path as follows:

```
make
./plain_benchmark -E 8 --dd --iwp ../K-V-Workload-Generator/workload.txt --qwp ../K-V-Workload-Generator/query_workload.txt --dw --dr
```
This will create a database in the current directory using path `./db_working_home`, populate the database using the ingestion workload, and then benchmark RocksDB using the query workload (`dw` and `dr` respectively represent direct write and direct read). The experiment results (e.g., latency or throughput) are printed when the benchmark finishes. More options are available. Type:

```
./plain_benchmark --help 
```

for more details.
