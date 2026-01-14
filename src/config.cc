#include "common/constants.h"
#include "transaction/transaction.h"

#include "gflags/gflags.h"

// -----------------------------------------------------------------------------------
/* System in general */
DEFINE_bool(uring_iopool, false,
            "Whether to enable IORING_SETUP_IOPOLL for all uring instances or not"
            "Require the block device (SSD) to support IOPOLL");
DEFINE_string(db_path, "/dev/s0", "Default block device");
DEFINE_uint32(worker_count, 16, "The number of workers");
DEFINE_uint32(page_provider_thread, 0, "Number of page provider threads");
DEFINE_bool(worker_pin_thread, false, "Pin worker to a specific thread");
DEFINE_uint32(txn_rate, 0, "Limit transaction rate in second for latency checking -- Disabled by default");
// -----------------------------------------------------------------------------------
/* Buffer manager */
DEFINE_uint64(bm_virtual_gb, 4, "Size of virtual memory in GB");
DEFINE_uint64(bm_physical_gb, 1, "Size of physical memory in GB");
DEFINE_uint64(bm_alias_block_mb, 1024, "Size of worker-local aliasing area in MB");
DEFINE_uint64(bm_evict_batch_size, 64, "Expected number of pages to be evicted during each eviction");
DEFINE_bool(bm_enable_fair_eviction, true,
            "Whether to use fair extent eviction policy or not"
            "Fair eviction policy: Large extents are more likely to be evicted than small extents/pages");
// -----------------------------------------------------------------------------------
/* Write-ahead logging */
DEFINE_bool(wal_enable, true, "Whether to enable WAL logging or not");
DEFINE_uint32(wal_variant, 2,
              "Which variant of decentralized logging is being used"
              "0. Original Global Sequence Number variant from Tianzheng Wang"
              "1. The Remote-Flush-Avoidance variant by Michael Haubenschild"
              "2. The GSN-vector proposal");
DEFINE_bool(wal_debug, false, "Enable debugging for WAL ops");
DEFINE_bool(wal_fsync, true, "Force FSync for WAL");
DEFINE_uint64(wal_buffer_size_mb, 10, "Size of WAL log buffer in MB");

/* Configuration for commit protocols */
DEFINE_uint32(wal_batch_write_kb, 16,
              "Workers only write WAL logs when the amount of unwritten logs are larger than this flag");
DEFINE_uint32(wal_block_size_mb, 10, "The write granularity for workers to avoid contention");
DEFINE_uint32(wal_max_idle_time_us, 30,
              "Force write when the avg idle time (from sampling) is larger than this number");
DEFINE_uint32(wal_stealing_group_size, 8,
              "The size (number of workers) of the stealing group, in which "
              "workers can steal log entries from other peers in the same group");

/* Recovery */
DEFINE_bool(wal_enable_recovery, false, "Whether to run recovery before 'boot up' the system");
DEFINE_bool(wal_instant_recovery, false, "Whether to use normal 3-phase recovery or instant recovery");
DEFINE_uint32(wal_recovery_threads, 1, "Number of threads used for recovery");
// -----------------------------------------------------------------------------------
/* Transaction */
DEFINE_bool(txn_debug, false, "Enable debugging for transaction ops, including commit latency info");
DEFINE_string(txn_default_isolation_level, "ru",
              "The serializable mode used in LeanStore"
              "(ru: READ_UNCOMMITTED, rc: READ_COMMITTED, si: SNAPSHOT_ISOLATION, ser: SERIALIZABLE)");

/* Configuration for commit processing subsystem */
DEFINE_int32(txn_commit_variant, static_cast<int>(leanstore::transaction::CommitProtocol::AUTONOMOUS_COMMIT),
             "Which commit strategy to be used, see transaction::CommitProtocol"
             "See class leanstore::transaction::CommitProtocol for your information");
DEFINE_uint32(txn_commit_group_size, 2,
              "The size (number of workers) of the commit group, in which workers of the same group can:"
              "- Workers in the same group trigger commit for the whole group directly");
DEFINE_uint32(txn_queue_size_mb, 10, "The transaction queue size in MB");
// -----------------------------------------------------------------------------------
/* BLOB */
DEFINE_bool(blob_enable, false, "Whether to enable Blob functionalities");
DEFINE_bool(blob_tail_extent, true, "Whether to enable Tail Extent or not");
DEFINE_bool(blob_normal_buffer_pool, true,
            "Extra overheads to emulate normal buffer pool, should be enabled during development"
            "1. *IMPORTANT* PageAliasGuard(): malloc() and memcpy() all the extents"
            "2. *IMPORTANT* Extra hashtable lookup on Buffer's ToPtr & Read op"
            "3. *IMPORTANT* Require chunked processing on large object operations"
            "4. GroupCommit::PrepareLargePageWrite: Write on 4KB granularity instead of extent granularity");
DEFINE_uint64(blob_buffer_pool_gb, 1,
              "Fixed size of the virtual memory range of BLOB in GBs"
              "0. Same value with FLAGS_bm_virtual_gb"
              "> 0. Fixed value");
