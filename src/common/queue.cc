#include "common/queue.h"
#include "common/rand.h"
#include "common/utils.h"
#include "sync/hybrid_guard.h"
#include "transaction/transaction.h"

#include <sys/mman.h>
#include <atomic>
#include <iostream>

namespace leanstore {

// ----------------------------------------------------------------------------------------------

template <typename T>
LockFreeQueue<T>::LockFreeQueue() : buffer_capacity_(FLAGS_txn_queue_size_mb * MB) {
  assert(std::is_trivially_destructible_v<T>);
  buffer_ = reinterpret_cast<u8 *>(AllocHuge(buffer_capacity_));
}

template <typename T>
LockFreeQueue<T>::~LockFreeQueue() {
  munmap(buffer_, buffer_capacity_);
}

/**
 * @brief Erase multiple items at once
 * `last_loop_bytes` starting from `head_.load()` should perfectly store `n_items` queued items
 * i.e., you should call SizeApprox() -> LoopElement() -> Erase():
 * - SizeApprox(): Return the number of queued items at the moment
 * - LoopElement(): Loop through all these elements,
 *    and retrieved total number of bytes these items consume, i.e., `last_loop_bytes`
 * - Erase(): Remove `n_items` from SizeApprox() and `no_bytes` from LoopElement()
 */
template <typename T>
void LockFreeQueue<T>::Erase(u64 n_items) {
  if (n_items <= 0) { return; }
  assert(last_loop_bytes > 0);
  auto r_head = head_.load();

  if (r_head + last_loop_bytes < buffer_capacity_) {
    r_head += last_loop_bytes;
  } else {
    r_head = last_loop_bytes - (buffer_capacity_ - r_head);
  }
  last_loop_bytes = 0;  // Reset the counter
  while (true) {
    auto w_tail_sz = tail_.load();
    auto w_tail    = w_tail_sz >> 32;
    auto cur_size  = static_cast<u64>(w_tail_sz & SIZE_MASK);
    assert(cur_size >= n_items);
    if (tail_.compare_exchange_weak(w_tail_sz, NewTail(w_tail, cur_size - n_items))) { break; }
  }
  head_.store(r_head);
}

/**
 * @brief Return the byte offset at which the loop stops
 */
template <typename T>
auto LockFreeQueue<T>::LoopElement(u64 no_elements, const std::function<bool(T &)> &fn) -> u64 {
  auto r_head   = head_.load();
  auto old_head = r_head;

  auto idx = 0UL;
  for (; idx < no_elements; idx++) {
    /* Circular back to the beginning of the buffer if deadend meet */
    if (T::InvalidByteBuffer(&buffer_[r_head])) { r_head = 0; }

    /* Read the queued item */
    const auto item = reinterpret_cast<T *>(&buffer_[r_head]);
    if (!fn(*item)) { break; }
    r_head += item->MemorySize();
  }

  last_loop_bytes = (r_head > old_head) ? r_head - old_head : r_head + buffer_capacity_ - old_head;
  return idx;
}

// ----------------------------------------------------------------------------------------------

template <class T>
auto ConcurrentQueue<T>::LoopElement(u64 no_elements, const std::function<bool(T &)> &fn) -> u64 {
  sync::HybridGuard guard(&latch_, sync::GuardMode::EXCLUSIVE);
  auto idx = 0UL;
  for (; idx < no_elements; idx++) {
    if (!fn(internal_[idx])) { break; }
  }
  return idx;
}

template <class T>
void ConcurrentQueue<T>::Push(T &element) {
  sync::HybridGuard guard(&latch_, sync::GuardMode::EXCLUSIVE);
  internal_.emplace_back(element);
}

template <class T>
auto ConcurrentQueue<T>::Erase(u64 no_elements) -> bool {
  sync::HybridGuard guard(&latch_, sync::GuardMode::EXCLUSIVE);
  if (internal_.size() < no_elements) { return false; }
  internal_.erase(internal_.begin(), internal_.begin() + no_elements);
  return true;
}

template <class T>
auto ConcurrentQueue<T>::SizeApprox() -> size_t {
  size_t ret = 0;

  while (true) {
    try {
      sync::HybridGuard guard(&latch_, sync::GuardMode::OPTIMISTIC);
      ret = internal_.size();
      break;
    } catch (const sync::RestartException &) {}
  }

  return ret;
}

// ----------------------------------------------------------------------------------------------

template class ConcurrentQueue<transaction::Transaction>;
template class LockFreeQueue<transaction::SerializableTransaction>;

}  // namespace leanstore