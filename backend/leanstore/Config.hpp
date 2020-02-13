#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DECLARE_double(dram_gib);  // 1 GiB
DECLARE_string(ssd_path);
DECLARE_uint32(worker_threads);
DECLARE_string(csv_path);
DECLARE_bool(csv_truncate);
DECLARE_string(free_pages_list_path);
DECLARE_uint32(cool_pct);
DECLARE_uint32(free_pct);
DECLARE_uint32(partition_bits);
DECLARE_uint32(async_batch_size);
DECLARE_uint32(falloc);
DECLARE_uint32(pp_threads);
DECLARE_bool(trunc);
DECLARE_bool(fs);
DECLARE_bool(root);
DECLARE_bool(print_debug);
DECLARE_uint32(print_debug_interval_s);
// -------------------------------------------------------------------------------------
DECLARE_bool(cm_split);
DECLARE_bool(cm_merge);
DECLARE_uint64(cm_update_tracker_pct);
// -------------------------------------------------------------------------------------
DECLARE_bool(su_merge);
// -------------------------------------------------------------------------------------
DECLARE_string(zipf_path);
DECLARE_double(zipf_factor);
DECLARE_double(target_gib);
DECLARE_uint64(run_for_seconds);
// -------------------------------------------------------------------------------------
DECLARE_uint64(backoff_strategy);
DECLARE_uint64(restarts_threshold);
// -------------------------------------------------------------------------------------
DECLARE_uint64(x);
DECLARE_uint64(y);
DECLARE_double(d);
// -------------------------------------------------------------------------------------
DECLARE_bool(bstar);
