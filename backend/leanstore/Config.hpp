#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DECLARE_double(dram_gib);  // 1 GiB
DECLARE_string(ssd_path);
DECLARE_uint32(worker_threads);
DECLARE_string(csv_dir);
DECLARE_string(file_suffix);
DECLARE_string(free_pages_list_path);
DECLARE_uint32(cool);
DECLARE_uint32(free);
DECLARE_uint32(partition_bits);
DECLARE_uint32(async_batch_size);
DECLARE_uint32(falloc);
DECLARE_uint32(pp_threads);
DECLARE_bool(trunc);
DECLARE_bool(root);
DECLARE_bool(print_debug);
DECLARE_uint32(print_debug_interval_s);
// -------------------------------------------------------------------------------------
DECLARE_string(zipf_path);
DECLARE_double(zipf_factor);
DECLARE_uint64(run_for_seconds);
