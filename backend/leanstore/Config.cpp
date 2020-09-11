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
DEFINE_uint32(pp_threads, 1, "number of page provider threads");
// -------------------------------------------------------------------------------------
DEFINE_string(csv_path, "./log", "");
DEFINE_bool(csv_truncate, false, "");
DEFINE_string(ssd_path, "./leanstore", "");
DEFINE_uint32(async_batch_size, 256, "");
DEFINE_bool(trunc, false, "Truncate file");
DEFINE_uint32(falloc, 0, "Preallocate GiB");
// -------------------------------------------------------------------------------------
DEFINE_bool(print_debug, true, "");
DEFINE_bool(print_tx_console, false, "");
DEFINE_uint32(print_debug_interval_s, 1, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(worker_threads, 20, "");
DEFINE_bool(pin_threads, false, "Responsibility of the driver");
DEFINE_bool(smt, true, "Simultaneous multithreading");
// -------------------------------------------------------------------------------------
DEFINE_bool(root, false, "does this process have root rights ?");
// -------------------------------------------------------------------------------------
DEFINE_uint64(backoff_strategy, 0, "");
// -------------------------------------------------------------------------------------
DEFINE_string(zipf_path, "/bulk/zipf", "");
DEFINE_double(zipf_factor, 0.0, "");
DEFINE_double(target_gib, 0.0, "size of dataset in gib (exact interpretation depends on the driver)");
DEFINE_uint64(run_for_seconds, 10, "Keep the experiment running for x seconds");
DEFINE_uint64(warmup_for_seconds, 10, "Warmup for x seconds");
// -------------------------------------------------------------------------------------
DEFINE_bool(cm_split, false, "");
DEFINE_uint64(cm_update_on, 7, "as exponent of 2");
DEFINE_uint64(cm_period, 14, "as exponent of 2");
DEFINE_uint64(cm_slowpath_threshold, 100, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(su_merge, false, "");
DEFINE_uint64(su_kwaymerge, 5, "");
DEFINE_double(su_target_pct, 80, "");
// -------------------------------------------------------------------------------------
DEFINE_uint64(backoff, 512, "");
DEFINE_bool(mutex, true, "");
// -------------------------------------------------------------------------------------
DEFINE_uint64(x, 512, "");
DEFINE_uint64(y, 100, "");
DEFINE_double(d, 0.0, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(bulk_insert, false, "");
// -------------------------------------------------------------------------------------
DEFINE_int64(trace_dt_id, -1, "");
DEFINE_int64(trace_trigger_probability, 100, "");
DEFINE_string(tag,"","Unique identifier for this, will be appended to each line csv");
