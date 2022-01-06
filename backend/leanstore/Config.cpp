// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_string(free_pages_list_path, "leanstore_free_pages", "");
// -------------------------------------------------------------------------------------
DEFINE_double(dram_gib, 1, "");
DEFINE_double(ssd_gib, 1700, "");
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
DEFINE_bool(print_tx_console, true, "");
DEFINE_uint32(print_debug_interval_s, 1, "");
DEFINE_bool(profiling, false, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(worker_threads, 4, "");
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
DEFINE_bool(contention_split, false, "");
DEFINE_uint64(cm_update_on, 7, "as exponent of 2");
DEFINE_uint64(cm_period, 14, "as exponent of 2");
DEFINE_uint64(cm_slowpath_threshold, 1, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(xmerge, false, "");
DEFINE_uint64(xmerge_k, 5, "");
DEFINE_double(xmerge_target_pct, 80, "");
// -------------------------------------------------------------------------------------
DEFINE_uint64(backoff, 512, "");
// -------------------------------------------------------------------------------------
DEFINE_uint64(x, 512, "");
DEFINE_uint64(y, 100, "");
DEFINE_double(d, 0.0, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(bulk_insert, false, "");
// -------------------------------------------------------------------------------------
DEFINE_int64(trace_dt_id, -1, "Print a stack trace for page reads for this DT ID");
DEFINE_int64(trace_trigger_probability, 100, "");
// -------------------------------------------------------------------------------------
DEFINE_string(tag, "", "Unique identifier for this, will be appended to each line csv");
// -------------------------------------------------------------------------------------
DEFINE_bool(out_of_place, false, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(wal, false, "");
DEFINE_uint64(wal_offset_gib, 1, "");
DEFINE_bool(wal_io_hack, false, "Does not really write logs on SSD");
DEFINE_bool(wal_fsync, false, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(si, false, "");
DEFINE_uint64(si_refresh_rate, 0, "");
DEFINE_bool(vw, false, "BTree with SI using versions in WAL");
DEFINE_bool(vw_todo, false, "");
DEFINE_bool(vi, false, "BTree with SI using in-place version");
// -------------------------------------------------------------------------------------
DEFINE_bool(persist, false, "");
DEFINE_bool(recover, false, "");
DEFINE_string(persist_file, "./leanstore.json", "Where should the persist config be saved to?");
DEFINE_string(recover_file, "./leanstore.json", "Where should the recover config be loaded from?");
DEFINE_uint64(tmp, 0, "");
