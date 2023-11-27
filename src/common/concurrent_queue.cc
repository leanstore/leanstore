#include "common/concurrent_queue.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "sync/hybrid_guard.h"

namespace leanstore {

template <class T>
void ConcurrentQueue<T>::Enqueue(const T &element) {
  sync::HybridGuard guard(&latch_, sync::GuardMode::EXCLUSIVE);
  internal_.push_back(element);
}

template <class T>
auto ConcurrentQueue<T>::Dequeue(T &out_element) -> bool {
  // Try Optimistic Check first
  if (SizeApprox() == 0) { return false; }

  // Maybe not empty, i.e. has a few elements, but was dequeue all before X-latch is acquired here
  sync::HybridGuard guard(&latch_, sync::GuardMode::EXCLUSIVE);
  if (internal_.empty()) { return false; }
  out_element = internal_.front();
  internal_.pop_front();
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

template class ConcurrentQueue<int>;
template class ConcurrentQueue<pageid_t>;

}  // namespace leanstore