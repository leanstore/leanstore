#pragma once

#include "sync/hybrid_latch.h"

#include <deque>

namespace leanstore {

template <class T>
class ConcurrentQueue {
 public:
  ConcurrentQueue()  = default;
  ~ConcurrentQueue() = default;

  void Enqueue(const T &element);
  auto Dequeue(T &out_element) -> bool;
  auto SizeApprox() -> size_t;

 private:
  std::deque<T> internal_;
  sync::HybridLatch latch_;
};

}  // namespace leanstore