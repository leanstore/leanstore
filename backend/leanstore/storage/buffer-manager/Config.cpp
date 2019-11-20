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
DEFINE_uint32(cool, 10, "Start cooling pages when <= x% are free");
DEFINE_uint32(free, 10, "pct");
// -------------------------------------------------------------------------------------
DEFINE_uint32(async_batch_size, 256, "");
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
   cool_pct = FLAGS_cool;
   free_pct = FLAGS_free;
   async_batch_size = FLAGS_async_batch_size;
}
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------