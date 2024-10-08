#pragma once

#include "gflags/gflags.h"

DECLARE_bool(ycsb_benchmark_fstat);
DECLARE_bool(ycsb_random_payload);
DECLARE_uint32(ycsb_record_count);
DECLARE_double(ycsb_zipf_theta);
DECLARE_uint32(ycsb_read_ratio);
DECLARE_uint64(ycsb_exec_seconds);
DECLARE_uint64(ycsb_payload_size);
DECLARE_uint64(ycsb_max_payload_size);
