#include "buffer/alias_area.h"
#include "common/exceptions.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "storage/blob/blob_state.h"

#include <atomic>
#include <cmath>
#include <utility>
#include <vector>

// Set the first ((i) % NO_BLOCKS_PER_LOCK) bits
#define SET_BITS(i) ((1UL << ((i) % NO_BLOCKS_PER_LOCK)) - 1)
#define ALIAS_LOCAL_PTR_START(w_id) ((w_id) * block_pg_cnt_)
#define ALIAS_AREA_CAPABLE(index, w_id) ((index) < ALIAS_LOCAL_PTR_START((w_id) + 1))

namespace leanstore::buffer {

AliasingArea::AliasingArea(u64 alias_block_size, u64 max_buffer_size, storage::Page *alias_ptr)
    : block_size_(alias_block_size),
      block_pg_cnt_(alias_block_size / PAGE_SIZE),
      no_blocks_(max_buffer_size / alias_block_size),
      no_locks_(std::ceil(static_cast<float>(no_blocks_) / NO_BLOCKS_PER_LOCK)),
      aliasing_area_(alias_ptr),
      shared_alias_area_(&alias_ptr[ALIAS_LOCAL_PTR_START(FLAGS_worker_count)]),
      shared_area_ptr_(0),
      range_locks_(no_locks_),
      acquired_locks_(FLAGS_worker_count) {
  local_area_ptr_.resize(FLAGS_worker_count);
  for (size_t idx = 0; idx < FLAGS_worker_count; idx++) { local_area_ptr_[idx] = ALIAS_LOCAL_PTR_START(idx); }
}

auto AliasingArea::RequestAliasingArea(u64 requested_size) -> storage::Page * {
  u64 required_page_cnt = storage::blob::BlobState::PageCount(requested_size);

  // Check if the worker-local aliasing area is big enough for this request
  if (ALIAS_AREA_CAPABLE(local_area_ptr_[worker_thread_id] + required_page_cnt, worker_thread_id)) {
    auto curr_index = local_area_ptr_[worker_thread_id];
    local_area_ptr_[worker_thread_id] += required_page_cnt;
    return &aliasing_area_[curr_index];
  }

  // There is no room left in worker-local aliasing area, fallback to shalas, i.e. shared-aliasing area
  u64 required_block_cnt = std::ceil(static_cast<float>(required_page_cnt) / block_pg_cnt_);
  u64 pos;      // Inclusive
  u64 new_pos;  // Exclusive

  while (true) {
    // Try to find a batch which is large enough to store `required_page_cnt`,
    //  i.e. consecutive of blocks which is bigger than `required_block_cnt`
    bool found_range;
    do {
      pos         = shared_area_ptr_.load();
      new_pos     = pos + required_block_cnt;
      found_range = new_pos <= no_blocks_;
      if (new_pos >= no_blocks_) { new_pos = 0; }
    } while (!shared_area_ptr_.compare_exchange_weak(pos, new_pos));

    // The prev shared_area_ptr_ is at the end of the shared alias area, and there is no room left for the
    // `requested_size`
    if (!found_range) { continue; }
    auto end_pos = (new_pos == 0) ? no_blocks_ : new_pos;
    Ensure(pos + required_block_cnt == end_pos);

    // Now try to acquire bits in [pos..pos + required_block_cnt)
    auto start_pos       = pos;
    auto acquire_success = ToggleShalasLocks(true, start_pos, end_pos);

    // Lock acquisition success, return the start_pid
    if (acquire_success) {
      acquired_locks_[worker_thread_id].emplace_back(pos, required_block_cnt);
      return &shared_alias_area_[pos * block_pg_cnt_];
    }

    // Lock acquisition fail, undo bits [pos..start_pos), and continue trying to acquire a suitable range
    ToggleShalasLocks(false, pos, start_pos);
  }

  UnreachableCode();
  return nullptr;  // This is purely to silent the compiler/clang-tidy warning
}

void AliasingArea::ReleaseAliasingArea() {
  if (local_area_ptr_[worker_thread_id] > ALIAS_LOCAL_PTR_START(worker_thread_id)) {
    local_area_ptr_[worker_thread_id] = ALIAS_LOCAL_PTR_START(worker_thread_id);
  }
  if (!acquired_locks_[worker_thread_id].empty()) {
    for (auto &[block_pos, block_cnt] : acquired_locks_[worker_thread_id]) {
      Ensure(ToggleShalasLocks(false, block_pos, block_pos + block_cnt));
    }
    acquired_locks_[worker_thread_id].clear();
  }
}

/**
 * @brief Either Set or Clear bits in [block_start..block_end] in shalas_lk_
 */
auto AliasingArea::ToggleShalasLocks(bool set_op, u64 &block_start, u64 block_end) -> bool {
  if (block_end > no_blocks_) { return false; }
  bool success = true;

  for (; block_start < block_end;) {
    // Detect the necessary bits for locking [pos..next_pos)
    auto next_pos = std::min(block_start + NO_BLOCKS_PER_LOCK - block_start % NO_BLOCKS_PER_LOCK, block_end);
    auto bits     = (next_pos % NO_BLOCKS_PER_LOCK == 0) ? std::numeric_limits<u64>::max() : SET_BITS(next_pos);
    if (block_start % NO_BLOCKS_PER_LOCK != 0) { bits -= SET_BITS(block_start); }

    // Try to acquire the necessary bits
    u64 old_lock;
    u64 x_lock;
    auto &lock = range_locks_[block_start / NO_BLOCKS_PER_LOCK];
    do {
      old_lock = lock.load();

      if (set_op) {
        x_lock = old_lock | bits;
        // If at least one of the bits was acquired by concurrent workers, abort
        if ((old_lock & bits) > 0) {
          success = false;
          break;
        }
      } else {
        x_lock = old_lock & ~bits;
      }
    } while (!lock.compare_exchange_weak(old_lock, x_lock));

    // The last lock acquisition fails, break to undo the prev lock acquisitions
    if (!success) { break; }

    // Move to next lock block
    block_start = next_pos;
  }

  return success;
}

}  // namespace leanstore::buffer