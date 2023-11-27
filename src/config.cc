#include "common/constants.h"

#include "gflags/gflags.h"

// -----------------------------------------------------------------------------------
/* System in general */
DEFINE_string(db_path, "/dev/database/main", "Default database path");
DEFINE_string(exmap_path, "/dev/exmap", "Default Exmap path");
DEFINE_uint32(worker_count, 16, "The number of workers");
DEFINE_uint32(page_provider_thread, 0, "Number of page provider threads");
DEFINE_bool(worker_pin_thread, false, "Pin worker to a specific thread");
// -----------------------------------------------------------------------------------
/* Buffer manager */
DEFINE_uint64(bm_virtual_gb, 4, "Size of virtual memory in GB");
DEFINE_uint64(bm_physical_gb, 1, "Size of physical memory in GB");
DEFINE_uint64(bm_wl_alias_mb, 1024, "Size of worker-local mmap() for aliasing in MB");
DEFINE_uint64(bm_evict_batch_size, 64, "Expected number of pages to be evicted during each eviction");
DEFINE_uint32(bm_aio_qd, 64, "Maximum number of concurrent I/O processed by bm's io_uring");
DEFINE_bool(bm_enable_fair_eviction, true,
            "Whether to use fair extent eviction policy or not"
            "Fair eviction policy: Large extents are more likely to be evicted than small extents/pages");
// -----------------------------------------------------------------------------------
/* Free Page manager */
DEFINE_bool(fp_enable, true, "Whether to enable free page manager or not");
// -----------------------------------------------------------------------------------
/* Write-ahead logging */
DEFINE_bool(wal_enable, true, "Whether to enable WAL logging or not");
DEFINE_bool(wal_enable_rfa, true, "Whether to enable Remote-Flush-Avoidance or not");
DEFINE_bool(wal_debug, false, "Enable debugging for WAL ops");
DEFINE_bool(wal_fsync, true, "Force FSync for WAL");
DEFINE_uint32(wal_max_qd, 256, "Queue depth of WAL's aio backend");
// -----------------------------------------------------------------------------------
/* Transaction */
DEFINE_bool(txn_debug, false, "Enable debugging for transaction ops");
DEFINE_string(txn_default_isolation_level, "ru",
              "The serializable mode used in LeanStore"
              "(ru: READ_UNCOMMITTED, rc: READ_COMMITTED, si: SNAPSHOT_ISOLATION, ser: SERIALIZABLE)");
// -----------------------------------------------------------------------------------
/* BLOB */
DEFINE_bool(blob_tail_extent, true, "Whether to enable Tail Extent or not");
DEFINE_bool(blob_normal_buffer_pool, false,
            "Extra overheads to emulate normal buffer pool"
            "1. *IMPORTANT* PageAliasGuard(): malloc() and memcpy() all the extents"
            "2. *IMPORTANT* Extra hashtable lookup on Buffer's ToPtr & Read op"
            "3. *IMPORTANT* Require chunked processing on large object operations"
            "4. GroupCommit::PrepareLargePageWrite: Write on 4KB granularity instead of extent granularity");
DEFINE_uint32(blob_compression, 0, "Which compression lib to be used(0: OFF, 1: FSST, 2: ZSTD, 3: LZ4)");
DEFINE_uint32(blob_log_segment_size, 1 * leanstore::KB,
              "If blob_logging_variant = -1, i.e. BLOB is written to WAL, the blob can be bigger than WAL buffer size."
              "In this case, LeanStore splits the BLOB into multiple segments, each is very small."
              "This flag is used to determine the size of those segments");
DEFINE_int32(blob_logging_variant, 1,
             "Which strategy to be used for writing blob to consecutive pages"
             "-1. BLOB is copied to buffer as normal DB pages, and also to WAL as in commercial DBMSes"
             "0. All pages are evicted after the transaction is committed"
             "1. BLOB is copied to those pages in buffer, and these pages are marked as UNLOCKED after commits"
             "2. BLOB is copied to those pages in buffer, and these pages are marked as MARKED after commits");
DEFINE_int32(blob_indexing_variant, 0,
             "Which strategy to be used for indexing blob"
             "0. Disable BLOB indexing"
             "1. Use BLOB Handler for indexing"
             "2. 1KB Prefix of BLOB is used for indexing");
// -----------------------------------------------------------------------------------
/* Compression config */
DEFINE_uint32(zstd_compress_level, 1, "The compression level of ZSTD used in LeanStore");
DEFINE_uint32(lz4_compress_acceleration, 1, "The acceleration value for LZ4_compress_fast call");