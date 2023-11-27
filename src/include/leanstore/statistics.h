#pragma once

#include "common/constants.h"
#include "common/typedefs.h"

#include <atomic>

namespace leanstore::statistics {

extern std::atomic<u64> total_txn_completed;
extern std::atomic<u64> txn_processed;
extern std::atomic<u64> precommited_txn_processed[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> precommited_rfa_txn_processed[MAX_NUMBER_OF_WORKER];

namespace buffer {
extern std::atomic<u64> read_cnt;
extern std::atomic<u64> write_cnt;
extern std::atomic<u64> evict_cnt;
}  // namespace buffer

namespace blob {
extern std::atomic<u64> blob_logging_io;
}  // namespace blob

namespace storage {
extern std::atomic<u64> free_size;
}

namespace recovery {
extern std::atomic<u64> gct_phase_1_ms;
extern std::atomic<u64> gct_phase_2_ms;
extern std::atomic<u64> gct_phase_3_ms;
extern std::atomic<u64> gct_write_bytes;
}  // namespace recovery

}  // namespace leanstore::statistics