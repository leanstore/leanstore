#include "leanstore/statistics.h"

#include <array>

namespace leanstore::statistics {

std::atomic<u64> total_committed_txn                                 = 0;
std::atomic<u64> txn_processed[MAX_NUMBER_OF_WORKER]                 = {};
std::atomic<u64> commit_rounds[MAX_NUMBER_OF_WORKER]                 = {};
std::vector<u64> txn_per_round[MAX_NUMBER_OF_WORKER]                 = {};
std::atomic<u64> precommited_txn_processed[MAX_NUMBER_OF_WORKER]     = {};
std::atomic<u64> precommited_rfa_txn_processed[MAX_NUMBER_OF_WORKER] = {};
std::vector<u64> txn_latency[MAX_NUMBER_OF_WORKER]                   = {};
std::vector<u64> rfa_txn_latency[MAX_NUMBER_OF_WORKER]               = {};
std::vector<u64> txn_lat_inc_wait[MAX_NUMBER_OF_WORKER]              = {};
std::vector<u64> txn_queue[MAX_NUMBER_OF_WORKER]                     = {};
std::vector<u64> txn_exec[MAX_NUMBER_OF_WORKER]                      = {};
std::array<i64, SAMPLING_SIZE> worker_idle_ns[MAX_NUMBER_OF_WORKER]  = {};

namespace buffer {
std::atomic<u64> read_cnt  = 0;
std::atomic<u64> write_cnt = 0;
std::atomic<u64> evict_cnt = 0;
}  // namespace buffer

namespace blob {
std::atomic<u64> blob_logging_io = 0;
std::atomic<u64> blob_alloc_try  = 0;
}  // namespace blob

namespace storage {
std::atomic<u64> free_size = 0;
}

namespace recovery {
std::atomic<u64> gct_phase_1_ns[MAX_NUMBER_OF_WORKER]    = {};
std::atomic<u64> gct_phase_2_ns[MAX_NUMBER_OF_WORKER]    = {};
std::atomic<u64> gct_phase_3_ns[MAX_NUMBER_OF_WORKER]    = {};
std::atomic<u64> gct_phase_4_ns[MAX_NUMBER_OF_WORKER]    = {};
std::atomic<u64> real_log_bytes[MAX_NUMBER_OF_WORKER]    = {};
std::atomic<u64> written_log_bytes[MAX_NUMBER_OF_WORKER] = {};
}  // namespace recovery

}  // namespace leanstore::statistics