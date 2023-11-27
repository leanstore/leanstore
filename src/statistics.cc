#include "leanstore/statistics.h"

namespace leanstore::statistics {

std::atomic<u64> total_txn_completed                                 = 0;
std::atomic<u64> txn_processed                                       = 0;
std::atomic<u64> precommited_txn_processed[MAX_NUMBER_OF_WORKER]     = {0};
std::atomic<u64> precommited_rfa_txn_processed[MAX_NUMBER_OF_WORKER] = {0};

namespace buffer {
std::atomic<u64> read_cnt  = 0;
std::atomic<u64> write_cnt = 0;
std::atomic<u64> evict_cnt = 0;
}  // namespace buffer

namespace blob {
std::atomic<u64> blob_logging_io = 0;
}  // namespace blob

namespace storage {
std::atomic<u64> free_size = 0;
}

namespace recovery {
std::atomic<u64> gct_phase_1_ms  = 0;
std::atomic<u64> gct_phase_2_ms  = 0;
std::atomic<u64> gct_phase_3_ms  = 0;
std::atomic<u64> gct_write_bytes = 0;
}  // namespace recovery

}  // namespace leanstore::statistics