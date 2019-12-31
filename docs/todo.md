* Contention management [random split, tuple movement]
* Backoff
* Page provider thread [monitoring, adaptive, copartitioning scalability]
* Shared latch
* ycsb

# Small talk
* AWS Batch
* White-box compression

# Last episode:
* Shared Latches (used only in scan atm)
* Partition buffer pool, cleaner page provider threads code (for each partiton, do all the phases), randomly pick a partition during allocation
* Remove free_threshold parameter, introduce pop() on allocation path and tryPop() on read path

* /dev/md0 manages 8,7 GiB/s with 120 threads io_benchmark
* /dev/md0 manages 8,4 GiB/s with 120 threads random_io
* /dev/md0 manages 8,96 GiB/s with 128 threads
# # TPC
* Skx: 20 threads, cool=20,free=1 -> 800K (in-memory) to 450K (out-of-memory) after 120 seconds. [20.csv]
* Rome: 120 threads, affinity, cool=20,free=1, 4 pp threads, 6 partition bits -> 5M (in-memory) to  1.37M(out-of-memory) after 300 seconds, writing ~3 GiB/s, reading ~2GiB/s [120ac.csv]

# Next ?
* Bulk loading ?
* Correct YCSB implementation (latest distribution and scans)
* Non clustered index on LeanStore ?
* HT for TID
* Contention management:
* *  VS BTree: payload
* *  8 Bytes TID
* *  Table Heap
* *  Update TID using shared guard and atomics ?
