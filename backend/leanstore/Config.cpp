// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_string(free_pages_list_path, "leanstore_free_pages", "");
// -------------------------------------------------------------------------------------
DEFINE_double(dram_gib, 1, "");  // 1 GiB
DEFINE_uint32(cool_pct, 10, "Start cooling pages when <= x% are free");
DEFINE_uint32(free_pct, 1, "pct");
DEFINE_uint32(partition_bits, 6, "bits per partition");
DEFINE_uint32(pp_threads, 4, "number of page provider threads");
// -------------------------------------------------------------------------------------
DEFINE_string(csv_path, "./log.csv", "");
DEFINE_string(ssd_path, "./leanstore", "");
DEFINE_uint32(async_batch_size, 256, "");
DEFINE_bool(trunc, false, "Truncate file");
DEFINE_uint32(falloc, 0, "Preallocate GiB");
// -------------------------------------------------------------------------------------
DEFINE_bool(print_debug, true, "");
DEFINE_uint32(print_debug_interval_s, 1, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(worker_threads, 20, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(root, false, "does this process have root rights ?");
DEFINE_bool(fs, false, "use fix\ed size btree");
// -------------------------------------------------------------------------------------
DEFINE_uint64(backoff_strategy, 0, "");
// -------------------------------------------------------------------------------------
DEFINE_string(zipf_path, "/bulk/zipf", "");
DEFINE_double(zipf_factor, 0.0, "");
DEFINE_double(target_gib, 0.0, "size of dataset in gib (exact interpretation depends on the driver)");
DEFINE_uint64(run_for_seconds, 10, "Keep the experiment running for x seconds");
// -------------------------------------------------------------------------------------
DEFINE_bool(cm_split, true, "");
DEFINE_bool(cm_merge, true, "");
DEFINE_uint64(cm_update_tracker_pct, 1, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(su_merge, false, "");
DEFINE_uint64(restarts_threshold, 100, "");
// -------------------------------------------------------------------------------------
DEFINE_uint64(x, 5, "");
DEFINE_uint64(y, 10, "");
DEFINE_double(d, 3.0, "");
// -------------------------------------------------------------------------------------
