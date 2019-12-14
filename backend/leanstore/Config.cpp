// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_string(free_pages_list_path, "leanstore_free_pages", "");
// -------------------------------------------------------------------------------------
DEFINE_double(dram_gib, 1, ""); // 1 GiB
DEFINE_uint32(cool, 10, "Start cooling pages when <= x% are free");
DEFINE_uint32(free, 10, "pct");
DEFINE_uint32(partition_bits, 4, "bits per partition");
// -------------------------------------------------------------------------------------
DEFINE_string(ssd_path, "./leanstore", "");
DEFINE_uint32(async_batch_size, 256, "");
DEFINE_bool(trunc, false, "Truncate file");
DEFINE_uint32(falloc, 0, "Preallocate GiB");
// -------------------------------------------------------------------------------------
DEFINE_bool(print_debug, true, "");
DEFINE_uint32(print_debug_interval_s, 1,"");
// -------------------------------------------------------------------------------------
