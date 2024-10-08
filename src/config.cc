#include "common/constants.h"
#include "transaction/transaction.h"

#include "gflags/gflags.h"

// -----------------------------------------------------------------------------------
/* System in general */
DEFINE_string(db_path, "/dev/database/main", "Default database path");
DEFINE_string(exmap_path, "/dev/exmap0", "Default Exmap path");
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
DEFINE_uint32(bm_aio_qd, 64, "Maximum number of concurrent I/O processed by bm's io_uring");
DEFINE_bool(bm_enable_fair_eviction, true,
            "Whether to use fair extent eviction policy or not"
            "Fair eviction policy: Large extents are more likely to be evicted than small extents/pages");
// -----------------------------------------------------------------------------------
/* Write-ahead logging */
DEFINE_bool(wal_enable, true, "Whether to enable WAL logging or not");
DEFINE_bool(wal_enable_rfa, true, "Whether to enable Remote-Flush-Avoidance or not");
DEFINE_bool(wal_debug, false, "Enable debugging for WAL ops");
DEFINE_bool(wal_fsync, true, "Force FSync for WAL");
DEFINE_uint64(wal_buffer_size_mb, 10, "Size of WAL log buffer in MB");

/* Configuration for commit protocols */
DEFINE_uint32(wal_batch_write_kb, 16,
              "Only used when FLAGS_txn_commit_variant in [WORKERS_WRITE_LOG, WILO_STEAL]"
              "Workers only write WAL logs when the amount of unwritten logs are larger than this flag");
DEFINE_uint32(wal_max_idle_time_us, 50,
              "Only used when FLAGS_txn_commit_variant in [WORKERS_WRITE_LOG, WILO_STEAL]"
              "Force write when the avg idle time (from sampling) is larger than this number");
DEFINE_uint32(wal_stealing_group_size, 8,
              "Only used when FLAGS_txn_commit_variant == WILO_STEAL"
              "The size (number of workers) of the commit group, in which workers of the same group can:"
              "- Steal log entries from other peers in the same group");
DEFINE_bool(wal_centralized_buffer, false,
            "Only used when FLAGS_txn_commit_variant in [BASELINE_COMMIT, FLUSH_PIPELINING]");
DEFINE_uint32(wal_force_commit_alignment, 4 * leanstore::KB,
              "Only used when FLAGS_txn_commit_variant in [WORKERS_WRITE_LOG, WILO_STEAL]"
              "Force log flush should align its I/O ops to this number");
// -----------------------------------------------------------------------------------
/* Transaction */
DEFINE_bool(txn_debug, false, "Enable debugging for transaction ops");
DEFINE_string(txn_default_isolation_level, "ru",
              "The serializable mode used in LeanStore"
              "(ru: READ_UNCOMMITTED, rc: READ_COMMITTED, si: SNAPSHOT_ISOLATION, ser: SERIALIZABLE)");
DEFINE_int32(txn_commit_variant, static_cast<int>(leanstore::transaction::CommitProtocol::FLUSH_PIPELINING),
             "Which commit strategy to be used, see transaction::CommitProtocol"
             "See class leanstore::transaction::CommitProtocol for your information");

/* Configuration for commit protocols */
DEFINE_uint32(txn_commit_group_size, 2,
              "Only used when FLAGS_txn_commit_variant == WILO_STEAL"
              "The size (number of workers) of the commit group, in which workers of the same group can:"
              "- Workers in the same group trigger commit for the whole group directly");
DEFINE_uint32(txn_queue_size_mb, 10,
              "The capacity of the lock-free queue in MB"
              "0 means disables optimized queue, i.e. use vector+mutex instead");
DEFINE_bool(txn_collect_state_during_flush, true,
            "Only used when FLAGS_txn_commit_variant == WILO_STEAL"
            "Whether we collect the consistent state for the subsequent commit round during log flush");
// -----------------------------------------------------------------------------------
/* BLOB */
DEFINE_bool(blob_enable, false, "Whether to enable Blob functionalities");
DEFINE_bool(blob_tail_extent, true, "Whether to enable Tail Extent or not");
DEFINE_bool(blob_normal_buffer_pool, false,
            "Extra overheads to emulate normal buffer pool"
            "1. *IMPORTANT* PageAliasGuard(): malloc() and memcpy() all the extents"
            "2. *IMPORTANT* Extra hashtable lookup on Buffer's ToPtr & Read op"
            "3. *IMPORTANT* Require chunked processing on large object operations"
            "4. GroupCommit::PrepareLargePageWrite: Write on 4KB granularity instead of extent granularity");
DEFINE_uint64(blob_buffer_pool_gb, 0,
              "Whether to use tier buffer manager or a fixed virtual memory range for BLOB allocation"
              "0. Use tier buffer manager for extent allocation"
              "> 0. Fixed virtual memory range of FLAGS_blob_buffer_size_gb GBs");
