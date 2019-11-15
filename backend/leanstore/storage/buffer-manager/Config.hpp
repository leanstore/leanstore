#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore{
namespace buffermanager{
// -------------------------------------------------------------------------------------
struct Config {
   u32 dram_pages_count;
   u32 ssd_pages_count;
   string ssd_path;
   string free_pages_list_path;
   u32 cooling_threshold;
   u32 background_write_sleep;
   u32 write_buffer_size;
   u32 async_batch_size;
   Config();
};
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
