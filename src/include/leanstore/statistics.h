#pragma once

#include "common/constants.h"
#include "common/typedefs.h"

#include <atomic>
#include <vector>

namespace leanstore::statistics {

extern std::atomic<u64> total_committed_txn;
extern std::atomic<u64> txn_processed[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> commit_rounds[MAX_NUMBER_OF_WORKER];
extern std::vector<u64> txn_per_round[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> precommited_txn_processed[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> precommited_rfa_txn_processed[MAX_NUMBER_OF_WORKER];
extern std::vector<u64> txn_latency[MAX_NUMBER_OF_WORKER];
extern std::vector<u64> rfa_txn_latency[MAX_NUMBER_OF_WORKER];
extern std::vector<u64> txn_lat_inc_wait[MAX_NUMBER_OF_WORKER];
extern std::vector<u64> txn_queue[MAX_NUMBER_OF_WORKER];
extern std::vector<u64> txn_exec[MAX_NUMBER_OF_WORKER];
extern std::array<i64, SAMPLING_SIZE> worker_idle_ns[MAX_NUMBER_OF_WORKER];

namespace buffer {
extern std::atomic<u64> read_cnt;
extern std::atomic<u64> write_cnt;
extern std::atomic<u64> evict_cnt;
}  // namespace buffer

namespace blob {
extern std::atomic<u64> blob_logging_io;
extern std::atomic<u64> blob_alloc_try;
}  // namespace blob

namespace storage {
extern std::atomic<u64> free_size;
}

namespace recovery {
extern std::atomic<u64> gct_phase_1_ns[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> gct_phase_2_ns[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> gct_phase_3_ns[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> gct_phase_4_ns[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> real_log_bytes[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> written_log_bytes[MAX_NUMBER_OF_WORKER];
}  // namespace recovery

}  // namespace leanstore::statistics