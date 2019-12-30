* Contention management [random split, tuple movement]
* Backoff
* Page provider thread [monitoring, adaptive, copartitioning scalability]
* Shared latch
* ycsb for rea


# Last episode:
* Shared Latches (used only in scan atm)
* Partition buffer pool, cleaner page provider threads code (for each partiton, do all the phases), randomly pick a partition during allocation
* Remove free_threshold parameter, introduce pop() on allocation path and tryPop() on read path

* /dev/md0 manages 8,7 GiB/s with 120 threads io_benchmark
* /dev/md0 manages 8,4 GiB/s with 120 threads random_io
* /dev/md0 manages 8,96 GiB/s with 128 threads


# Next ?
* Bulk loading ?
* Correct YCSB implementation (latest distribution and scans)
* Contention management:
* *  VS BTree: payload
* *  8 Bytes TID
* *  Table Heap
* *  Update TID using shared guard and atomics ?
