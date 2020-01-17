// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_string(free_pages_list_path, "leanstore_free_pages", "");
// -------------------------------------------------------------------------------------
DEFINE_double(dram_gib, 1, "");  // 1 GiB
DEFINE_uint32(cool, 10, "Start cooling pages when <= x% are free");
DEFINE_uint32(free, 1, "pct");
DEFINE_uint32(partition_bits, 4, "bits per partition");
DEFINE_uint32(pp_threads, 1, "number of page provider threads");
// -------------------------------------------------------------------------------------
DEFINE_string(ssd_path, "./leanstore", "");
DEFINE_string(file_suffix, "d", "");
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
// -------------------------------------------------------------------------------------
DEFINE_string(zipf_path, "/bulk/zipf", "");
DEFINE_double(zipf_factor, 0.0, "");
DEFINE_uint64(run_for_seconds, 10, "Keep the experiment running for x seconds");
