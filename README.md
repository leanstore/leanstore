# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling

Install dependencies:

```
sudo ./toolbox/pkgs/ubuntu-focal.sh
```

Build LeanStore using the mk-utility:

```
make common
```

or instrument CMake directly:

```
mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
```

## Benchmark Examples
### TPC-C

`build/frontend/tpcc --tpcc_warehouse_count=100 --notpcc_warehouse_affinity --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=240 --csv_path=./log --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60 --isolation_level=si`

check `build/frontend/tpcc --help` for other options

### YCSB

`build/frontend/tpcc --ycsb_read_ratio=50 --target_gib=10 --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=5 --csv_path=./log --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60 --isolation_level=si`

check `build/frontend/ycsb --help` for other options

## Implement Your Own Workload

LeanStore offers a flexible transactional Key/Value interface similar to WiredTiger and RocksDB.
A table is a B-Tree index where keys and values are stored in a normalized format, i.e., lexicographically ordered strings.
For convenience, frontend/shared offers templates that take care of (un)folding common types.
The best starting points are frontend/minimal-example and frontend/ycsb.
The required parameters at runtime are: `--ssd_path=/block_device/or/filesystem --dram_gib=fixed_in_gib`.
The default tranasction isolation level is `--isolation_level=si`. You can lower it to Read Committed or Read Uncommitted by replaced `si` with `rc` or `ru` respectively.
You can set the transaction isolation level using `--isolation_level=si` and enable the B-Tree techniques from CIDR202 with `--contention_split --xmerge`.

### Metrics Reporting
LeanStore emits several metrics per second in CSV files: `log_bm.csv, log_configs.csv, log_cpu.csv, log_cr.csv, log_dt.csv`.
Each row has a c_hash value, which is calculated by chaining and hashing all the configurations that you passed to the binary at runtime.
This gives you an easy way to identify your run and join all relevant information from the different CSV files using SQLite, for example."

## Implemented Featuers

- [x] Lightweight buffer manager with pointer swizzling [ICDE18]
- [x] Optimstic Lock Coupling with Hybrid Page Guard to synchronize paged data structures [IEEE19]
- [x] Contention and Space Management in B-Trees [CIDR21]
- [x] Variable-length key/values B-Tree with prefix compression and hints  [BTW23]
- [x] Scalable and robust out-of-memory Snapshot Isolation (OSIC protocol, Graveyard and FatTuple) [VLDB23]
- [x] Distributed Logging with remote flush avoidance [SIGMOD20, BTW23]
- [ ] Recovery [SIGMOD20]
- [ ] What Modern NVMe Storage Can Do, And How To Exploit It: High-Performance {I/O} for High-Performance Storage Engines [VLDB23] [branch](https://github.com/leanstore/leanstore/tree/io)

## Cite

The code we used for our VLDB 2023 that covers alternative SI commit protocols is in a different [branch](https://github.com/leanstore/leanstore/tree/mvcc).

```BibTeX
@inproceedings{alhomssi23,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Scalable and Robust Snapshot Isolation for High-Performance Storage Engines},
    booktitle = {VLDB},
    year      = {2023}
}
```

The code we used for our VLDB 2023 that covers the fast I/O implementation is in a different [branch](https://github.com/leanstore/leanstore/tree/io).

```BibTeX
@article{haas23,
  author       = {Gabriel Haas and Viktor Leis},
  title        = {What Modern NVMe Storage Can Do, And How To Exploit It: High-Performance {I/O} for High-Performance Storage Engines},
  journal      = {Proc. {VLDB} Endow.},
  year         = {2023}
}
```

BTW 2023 [branch](https://github.com/leanstore/leanstore/tree/btw) that covers alternative dependency tracking.

```BibTeX
@inproceedings{leanstore23,
    author    = {Adnan Alhomssi, Michael Haubenschild and Viktor Leis},
    title     = {The Evolution of LeanStore},
    booktitle = {BTW},
    year      = {2023}
}
```

CIDR 2021 [branch](https://github.com/leanstore/leanstore/tree/cidr) (outdated).

```BibTeX
@inproceedings{alhomssi21,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Contention and Space Management in B-Trees},
    booktitle = {CIDR},
    year      = {2021}
}
```
