#pragma once

#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "sync/hybrid_latch.h"

#include "gtest/gtest_prod.h"

#include <array>
#include <atomic>
#include <cassert>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <type_traits>

namespace leanstore {

template <typename T>
class LockFreeQueue {
 public:
  static constexpr u64 SIZE_MASK = (static_cast<u64>(1) << 32) - 1;

  LockFreeQueue();
  ~LockFreeQueue();

  constexpr auto SizeApprox() -> u32 {
    auto w_tail_sz = tail_.load();
    return static_cast<u32>(w_tail_sz & SIZE_MASK);
  }

  auto NewTail(u64 w_tail, u64 size) -> u64 { return (w_tail << 32) + size; }

  /**
   * @brief Push a serialized element to the queue from unserialized data
   */
  template <typename T2>
  void Push(const T2 &element) {
    auto item_size = static_cast<uoffset_t>(element.SerializedSize());
    auto w_tail_sz = tail_.load();
    auto w_tail    = w_tail_sz >> 32;
    auto cur_size  = static_cast<u64>(w_tail_sz & SIZE_MASK);

    /* Circular buffer: no room for this element + a CR entry, so we circular back */
    if (buffer_capacity_ - w_tail < item_size + sizeof(T::NULL_ITEM)) {
      Ensure(buffer_capacity_ - w_tail >= sizeof(T::NULL_ITEM));
      std::memcpy(&buffer_[w_tail], &(T::NULL_ITEM), sizeof(T::NULL_ITEM));
      w_tail = 0;
    }

    /* Wait until the consumer accesses more items to free up some memory */
    auto r_head = head_.load();
    while (ContiguousFreeBytes(r_head, w_tail) < item_size) {
      r_head = head_.load();
      AsmYield();
    }

    /* Have enough memory -> producer write the element to the buffer */
    Ensure(w_tail % CPU_CACHELINE_SIZE == 0);
    auto obj = reinterpret_cast<T *>(&buffer_[w_tail]);
    obj->Construct(element);

    /* Update new tail value, which also contains number of queued items */
    while (!tail_.compare_exchange_weak(w_tail_sz, NewTail(w_tail + item_size, cur_size + 1))) {
      w_tail_sz = tail_.load();
      Ensure(((w_tail_sz >> 32) == w_tail) || (w_tail == 0));
      cur_size = static_cast<u64>(w_tail_sz & SIZE_MASK);
    }
  }

  /* Erase and loop utilities */
  void Erase(u64 n_items);
  auto LoopElement(u64 no_elements, const std::function<bool(T &)> &fn) -> u64;

 private:
  FRIEND_TEST(TestQueue, BasicTest);
  FRIEND_TEST(TestQueue, ConcurrencyTest);

  u8 *buffer_;
  u64 buffer_capacity_;
  /* Read from head, write to tail */
  std::atomic<u64> head_                         = {0};
  std::atomic<u64> tail_                         = {0}; /* First 32-bit are for tail, rest are for current size */
  inline static thread_local u64 last_loop_bytes = 0;   /* # bytes that the last LoopElement() go through */

  constexpr auto ContiguousFreeBytes(u64 r_head, u64 w_tail) -> u64 {
    return (w_tail < r_head) ? r_head - w_tail : buffer_capacity_ - w_tail;
  }
};

template <class T>
class ConcurrentQueue {
 public:
  ConcurrentQueue()  = default;
  ~ConcurrentQueue() = default;

  /**
   * @brief Unsafe operator[], assuming that size of the internal deque is larger than the idx
   * Only used for testing
   */
  auto operator[](u64 idx) -> T & { return internal_[idx]; }

  auto LoopElement(u64 no_elements, const std::function<bool(T &)> &fn) -> u64;
  void Push(T &element);
  auto Erase(u64 no_elements) -> bool;
  auto SizeApprox() -> size_t;

 private:
  std::deque<T> internal_;
  sync::HybridLatch latch_;
};

}  // namespace leanstore