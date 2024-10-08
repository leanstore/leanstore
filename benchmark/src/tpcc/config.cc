#include "benchmark/tpcc/config.h"

DEFINE_bool(tpcc_warehouse_affinity, false, "Whether to pin warehouse to a specific worker");
DEFINE_uint32(tpcc_warehouse_count, 4, "Number of TPC-C warehouses");
DEFINE_uint64(tpcc_exec_seconds, 20, "Execution time");