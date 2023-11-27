#include "buffer/resident_set.h"
#include "common/exceptions.h"
#include "common/utils.h"

#include "share_headers/logger.h"

#include <sys/mman.h>
#include <bit>
#include <cassert>
#include <cstring>
#include <mutex>
#include <stdexcept>

namespace leanstore::buffer {

ResidentPageSet::ResidentPageSet(u64 max_count, sync::PageState *page_state)
    : capacity_(max_count),
      count_(std::bit_ceil(static_cast<u64>(capacity_ * 1.5))),
      mask_(count_ - 1),
      clock_pos_(0),
      page_state_(page_state) {
  LOG_DEBUG("Initialize replacer with %lu slots", count_);
  slots_ = static_cast<Entry *>(AllocHuge(count_ * sizeof(Entry)));
  memset(static_cast<void *>(slots_), 0xFF, count_ * sizeof(Entry));
}

ResidentPageSet::~ResidentPageSet() {
  munmap(slots_, count_ * sizeof(u64));
  slots_ = nullptr;  // this trivial line is simply to quiet clang-tidy
}

auto ResidentPageSet::Capacity() -> u64 { return capacity_; }

auto ResidentPageSet::Contain(pageid_t page_id) -> bool {
  u64 pos = HashFn(page_id) & mask_;
  while (true) {
    u64 curr = slots_[pos].pid.load();
    if (curr == ResidentPageSet::EMPTY) { return false; }

    if (curr == page_id) { return true; }

    pos = (pos + 1) & mask_;
  }

  // Can't find the page_id in the hash table
  return false;
}

void ResidentPageSet::Insert(pageid_t page_id) {
  assert(GetPageState(page_id).LockState() == sync::PageStateMode::EXCLUSIVE);

  u64 pos = HashFn(page_id) & mask_;
  while (true) {
    u64 curr = slots_[pos].pid.load();
    if (curr == page_id) { UnreachableCode(); }
    if ((curr == ResidentPageSet::EMPTY) || (curr == ResidentPageSet::TOMBSTONE)) {
      if (slots_[pos].pid.compare_exchange_strong(curr, page_id)) { return; }
    }

    pos = (pos + 1) & mask_;
  }
}

auto ResidentPageSet::Remove(pageid_t page_id) -> bool {
  assert(GetPageState(page_id).LockState() == sync::PageStateMode::EXCLUSIVE);

  u64 pos = HashFn(page_id) & mask_;
  while (true) {
    u64 curr = slots_[pos].pid.load();
    if (curr == ResidentPageSet::EMPTY) { return false; }

    if (curr == page_id) {
      if (slots_[pos].pid.compare_exchange_strong(curr, ResidentPageSet::TOMBSTONE)) { return true; }
    }

    pos = (pos + 1) & mask_;
  }

  // Can't find the page_id in the hash table
  return false;
}

void ResidentPageSet::IterateClockBatch(u64 batch, const std::function<void(pageid_t)> &evict_fn) {
  u64 pos;
  u64 new_position;

  do {
    pos          = clock_pos_.load();
    new_position = (pos + batch) % count_;
  } while (!clock_pos_.compare_exchange_strong(pos, new_position));

  for (u64 idx = 0; idx < batch; idx++) {
    u64 curr = slots_[pos].pid.load();
    if ((curr != ResidentPageSet::TOMBSTONE) && (curr != ResidentPageSet::EMPTY)) { evict_fn(curr); }

    pos = (pos + 1) & mask_;
  }
}

}  // namespace leanstore::buffer