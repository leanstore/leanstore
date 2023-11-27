#pragma once

#include "gflags/gflags.h"
#include "share_headers/config.h"

DECLARE_string(exmap_path);
// -------------------------------------------------------------------------------------
DECLARE_uint32(worker_count);
DECLARE_uint32(page_provider_thread);
DECLARE_bool(worker_pin_thread);
// -------------------------------------------------------------------------------------
DECLARE_uint64(bm_virtual_gb);
DECLARE_uint64(bm_wl_alias_mb);
DECLARE_uint64(bm_evict_batch_size);
DECLARE_uint32(bm_aio_qd);
DECLARE_bool(bm_enable_fair_eviction);
// -------------------------------------------------------------------------------------
DECLARE_bool(fp_enable);
// -------------------------------------------------------------------------------------
DECLARE_bool(wal_enable);
DECLARE_bool(wal_enable_rfa);
DECLARE_bool(wal_debug);
DECLARE_bool(wal_fsync);
DECLARE_uint32(wal_max_qd);
// -------------------------------------------------------------------------------------
DECLARE_bool(txn_debug);
// -------------------------------------------------------------------------------------
DECLARE_bool(gc_enable);
// -----------------------------------------------------------------------------------
DECLARE_bool(blob_tail_extent);
DECLARE_bool(blob_normal_buffer_pool);
DECLARE_uint32(blob_compression);
DECLARE_uint32(blob_log_segment_size);
DECLARE_int32(blob_logging_variant);
DECLARE_int32(blob_indexing_variant);
// -----------------------------------------------------------------------------------
DECLARE_uint32(zstd_compress_level);
DECLARE_uint32(lz4_compress_acceleration);