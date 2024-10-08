#pragma once

#include "common/constants.h"
#include "common/typedefs.h"

#include <array>
#include <atomic>
#include <memory>
#include <vector>

namespace leanstore::sync {

struct EpochHandler {
  static constexpr u64 MAX_VALUE = ~0ULL;

  explicit EpochHandler(u64 no_threads);
  ~EpochHandler();

  void EpochOperation(wid_t wid);
  void DeferFreePointer(wid_t wid, void *ptr);

  /* Epoch-based ptr reclaimation */
  const u64 no_threads;
  std::atomic<u64> global_epoch;
  std::vector<std::atomic<u64>> local_epoch;
  std::array<std::vector<std::pair<void *, u64>>, MAX_NUMBER_OF_WORKER> to_free_ptr = {};
};

class EpochGuard {
 public:
  EpochGuard(std::atomic<u64> *epoch, const std::atomic<u64> &global_epoch) : epoch_(epoch) {
    epoch->store(global_epoch.load());
  }

  ~EpochGuard() { epoch_->store(EpochHandler::MAX_VALUE); }

 private:
  std::atomic<u64> *epoch_;
};

}  // namespace leanstore::sync