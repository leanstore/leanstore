#include "Config.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_uint32(dram, 65536, ""); // 1 GiB
DEFINE_uint32(ssd, 655360, ""); // 10 GiB
DEFINE_string(ssd_path, "leanstore_data", "");
DEFINE_string(free_pages_list_path, "leanstore_free_pages", "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(cooling_threshold, 10, "Start cooling pages when <= x% are free");
// -------------------------------------------------------------------------------------
DEFINE_uint32(evict_cooling_threshold, 10, "pct");
DEFINE_uint32(write_buffer_size, 512, "");
DEFINE_uint32(async_batch_size, 128, "");
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
Config::Config()
{
   dram_pages_count = FLAGS_dram;
   ssd_pages_count = FLAGS_ssd;
   ssd_path = FLAGS_ssd_path;
   free_pages_list_path = FLAGS_free_pages_list_path;
   cooling_threshold = FLAGS_cooling_threshold;
   evict_cooling_threshold = FLAGS_evict_cooling_threshold;
   write_buffer_size = FLAGS_write_buffer_size;
   async_batch_size = FLAGS_async_batch_size;
}
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------