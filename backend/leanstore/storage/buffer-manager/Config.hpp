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
   u32 cool_pct;
   u32 free_pct;
   u32 async_batch_size;
   Config();
};
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
