#include "sync/epoch_handler.h"
#include "common/rand.h"

namespace leanstore::sync {

EpochEntry::EpochEntry(void *ptr, u64 epoch, void (*d)(void *)) : ptr(ptr), epoch(epoch), deleter(d) {}

EpochHandler::EpochHandler(u64 no_threads) : no_threads(no_threads), global_epoch(0), local_epoch(no_threads) {}

EpochHandler::~EpochHandler() {
  /* Free existing to_free pointers */
  for (auto &ptr_list : to_free_ptr) {
    for (auto &entry : ptr_list) { entry.deleter(entry.ptr); }
  }
}

void EpochHandler::EpochOperation(wid_t wid) {
  /* Increase epoch*/
  if (Rand(200) % 200 == 0) { global_epoch++; }

  /* Pointer reclaimation */
  if (Rand(100) % 100 == 0) {
    auto min_epoch = MAX_VALUE;
    for (const auto &epoch : local_epoch) { min_epoch = std::min(min_epoch, epoch.load()); }
    auto idx = 0ULL;
    for (; idx < to_free_ptr[wid].size(); idx++) {
      auto &entry = to_free_ptr[wid][idx];
      if (entry.epoch >= min_epoch) { break; }
      entry.deleter(entry.ptr);
    }
    to_free_ptr[wid].erase(to_free_ptr[wid].begin(), to_free_ptr[wid].begin() + idx);
  }
}

void EpochHandler::DeferFreePointer(wid_t wid, void *ptr, void (*d)(void *)) {
  to_free_ptr[wid].emplace_back(ptr, local_epoch[wid].load(), d);
}

}  // namespace leanstore::sync