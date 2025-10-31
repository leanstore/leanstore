# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Requirements

### Hardware

- Multiple-core CPUs, best with ~ 100 cores
- [A battery-backed NVMe SSD](https://www.memorysolution.de/produkte-online-shop/ssd-hdd/datacenter-ssd)
  - If you only have a consumer SSD (i.e., does not come with built-in battery), you can simulate the performance of the battery-backed NVMe SSD with parameter `-wal_fsync=false`

### Software

- **Requirement**: LeanStore depends on `exmap`, which is currently fixed with Linux kernel 6.11
  - If anyone wants to test LeanStore with other kernel, please [send an email to the author](mailto:lamduy.nguyen@tum.de)

## Implemented Featuers

- [x] Autonomous commit and barrier transactions [SIGMOD'25]
- [x] Variable-sized objects, File system interface, and virtual-memory aliasing [ICDE'24]
- [x] Virtual-memory assisted buffer manager with explicit OS pagetable management [SIDMOG'23]
- [x] Optimstic Lock Coupling with Hybrid Page Guard to synchronize paged data structures [IEEE'19]
- [x] Variable-length key/values B-Tree with prefix compression and hints [BTW'23]
- [x] Distributed Logging with remote flush avoidance [SIGMOD'20]
- [ ] Recovery [SIGMOD'20]

## Dependencies

### Core

`sudo apt-get install cmake libtbb-dev libfmt-dev libgflags-dev libgtest-dev libgmock-dev libgcrypt-dev liburing-dev libzstd-dev libbenchmark-dev libssl-dev`

**exmap**: stored in `share_libs/exmap`
- Run `sudo ./load.sh`

### Third-party databases

**Databases**: `sudo apt-get install libwiredtiger-dev libsqlite3-dev librocksdb-dev libmysqlcppconn-dev libpq-dev libfuse-dev`

### Misc

- To test/evaluate `exmap`, you may want to install libboost: `sudo apt-get install libboost-dev`

## Usage

### Compiling

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

### Testing

`cd build && cmake -DENABLE_TESTING=On .. && make test`

## Reproducibility

Paper: [Moving on From Group Commit: Autonomous Commit Enables High Throughput and Low Latency on NVMe SSDs](https://dl.acm.org/doi/abs/10.1145/3725328)

All experiments in the paper can be executed with `build/benchmark/LeanStore_TPCC`, `build/benchmark/LeanStore_YCSB`, or `build/benchmark/LeanStore_TATP` executables and different parameters.

## Sample benchmark

**Stress benchmark with TPC-C**: `./benchmark/LeanStore_TPCC -worker_count=16 -tpcc_warehouse_count=32 -worker_pin_thread=true -tpcc_exec_seconds=20 -bm_virtual_gb=128 -bm_physical_gb=32 -db_path=/dev/nvme1n1 -txn_debug=true -txn_commit_variant=3`
  - Commit protocol (i.e., `-txn_commit_variant` parameter): Autonomous commit
  - Number of threads: 16
  - Number of TPC-C warehouseds: 32
  - Buffer pool size: 32 GB
  - SSD path: `/dev/nvme1n1`
    - *IMPORTANT*: Only support block device file
