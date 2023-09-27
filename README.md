# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## WATT Artifacts Overview

This sections provides an overview of all artifacts available for WATT.

### VLDB 2023

For the VLDB 2023 you can access our [paper](https://www.vldb.org/pvldb/vol16/p3323-vohringer.pdf) and a video of an early stage of the [presentation](https://youtu.be/ZwQFDwTbqS4?).

The [presentation](VLDB2023/presentation.pdf) itself and the [transcript](VLDB2023/presentation_transcript.md) can be found in the directory [VLDB2023](VLDB2023).

### Implementation

The implementation of WATT can be found in this repository.

Important parts are

1. Tracker in the [BufferFrame](https://github.com/leanstore/leanstore/blob/WATT/backend/leanstore/storage/buffer-manager/BufferFrame.hpp)
2. Replacement in the  [Page Evictor](https://github.com/leanstore/leanstore/blob/WATT/backend/leanstore/storage/buffer-manager/PageProviderThread.cpp)

### Simulation

We compared WATT with other strategies based on this simulation:

https://github.com/itodnerd/WATT-simulate

### Traces

For the simulation we used these traces:

https://github.com/itodnerd/WATT-traces/tree/main/WATT_competition_traces

### Cite WATT

```bibtex
@article{WATT23,
author = {V\"{o}hringer, Demian and Leis, Viktor},
title = {Write-Aware Timestamp Tracking: Effective and Efficient Page Replacement for Modern Hardware},
year = {2023},
issue_date = {July 2023},
publisher = {VLDB Endowment},
volume = {16},
number = {11},
issn = {2150-8097},
url = {https://doi.org/10.14778/3611479.3611529},
doi = {10.14778/3611479.3611529},
journal = {Proc. VLDB Endow.},
month = {aug},
pages = {3323–3334},
numpages = {12}
}
```

## Compiling

Install dependencies:

`sudo apt-get install cmake libaio-dev libtbb-dev`

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

## TPC-C Example

`build/frontend/tpcc --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=240 --tpcc_warehouse_count=100 --notpcc_warehouse_affinity --csv_path=./log --cool_pct=40 --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60`

check `build/frontend/tpcc --help` for other options

## Cite
The code we used for our CIDR 2021 paper is in a different (and outdated) [branch](https://github.com/leanstore/leanstore/tree/cidr).

```bibtex
@inproceedings{alhomssi21,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Contention and Space Management in B-Trees},
    booktitle = {CIDR},
    year      = {2021}
}
```
