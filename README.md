# LeanStore
[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling
Install dependencies:

`sudo apt-get install cmake libaio-dev libtbb-dev`

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

## TPC-C Example
`build/frontend/tpcc --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=240 --tpcc_warehouse_count=100 --notpcc_warehouse_affinity --csv_path=./log --cool_pct=40 --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60 --isolation_level=si`

check `build/frontend/tpcc --help` for other options

## Implemented Featuers
- [x] Lightweight buffer manager with pointer swizzling [ICDE18]
- [x] Optimstic Lock Coupling with Hybrid Page Guard to synchronize paged data structures [IEEE19]
- [x] Variable-length key/values B-Tree with prefix compression and hints  [BTW23]
- [x] Scalable and robust out-of-memory Snapshot Isolation (OSIC protocol, Graveyard and FatTuple) [VLDB23]
- [x] Distributed Logging with remote flush avoidance [SIGMOD20, BTW23]
- [ ] Recovery [SIGMOD20]

## Cite
The code we used for our VLDB 2023 that covers alternative SI commit protocols is in a different [branch](https://github.com/leanstore/leanstore/tree/mvcc).
```
@inproceedings{alhomssi23,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Scalable and Robust Snapshot Isolation for High-Performance Storage Engines},
    booktitle = {VLDB},
    year      = {2023}
}
```

BTW 2023 [branch](https://github.com/leanstore/leanstore/tree/btw) that covers alternative dependency tracking.
```
@inproceedings{leanstore23,
    author    = {Adnan Alhomssi, Michael Haubenschild and Viktor Leis},
    title     = {The Evolution of LeanStore},
    booktitle = {BTW},
    year      = {2023}
}
```

CIDR 2021 [branch](https://github.com/leanstore/leanstore/tree/cidr) (outdated).
```
@inproceedings{alhomssi21,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Contention and Space Management in B-Trees},
    booktitle = {CIDR},
    year      = {2021}
}
```
