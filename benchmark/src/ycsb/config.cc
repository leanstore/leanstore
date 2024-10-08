#include "benchmark/ycsb/config.h"

DEFINE_bool(ycsb_benchmark_fstat, false, "Whether to use normal key-value benchmark or to use fstat() scan");
DEFINE_bool(ycsb_random_payload, false, "Whether to use random payload for YCSB");
DEFINE_uint32(ycsb_record_count, 100000, "Number of initial records");
DEFINE_double(ycsb_zipf_theta, 0, "The zipfian dist's theta parameter");
DEFINE_uint32(ycsb_read_ratio, 100, "Read ratio");
DEFINE_uint64(ycsb_exec_seconds, 20, "Execution time");
DEFINE_uint64(ycsb_payload_size, 120,
              "Size of key-value payload. Only support 120 bytes (default), 4KB, 100KB, and 10MB");
DEFINE_uint64(ycsb_max_payload_size, 10485760, "Maximum size of payload in case FLAGS_ycsb_random_payload==true");