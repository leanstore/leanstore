#include "sync/hybrid_latch.h"
#include "common/utils.h"
#include "sync/page_state.h"

#include <cassert>

namespace leanstore::sync {

template <class SyncStateClass>
HybridLatchImpl<SyncStateClass>::HybridLatchImpl() {
  state_and_version_.store(SameVersionNewState(0, SyncStateClass::UNLOCKED), std::memory_order_release);
}

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::LockState(u64 v) -> u64 {
  return v >> 56;
};

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::Version(u64 v) -> u64 {
  return static_cast<u64>(v & VERSION_MASK);
};

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::LockState() -> u64 {
  return LockState(state_and_version_.load());
}

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::Version() -> u64 {
  return Version(state_and_version_.load());
}

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::StateAndVersion() -> std::atomic<u64> & {
  return state_and_version_;
}

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::TryLockExclusive() -> bool {
  auto old_state_w_version = state_and_version_.load();
  return TryLockExclusive(old_state_w_version);
}

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::TryLockExclusive(u64 old_state_w_version) -> bool {
  if (LockState(old_state_w_version) > SyncStateClass::UNLOCKED &&
      LockState(old_state_w_version) <= SyncStateClass::EXCLUSIVE) {
    return false;
  }
  return state_and_version_.compare_exchange_strong(
    old_state_w_version, SameVersionNewState(old_state_w_version, SyncStateClass::EXCLUSIVE));
}

template <class SyncStateClass>
void HybridLatchImpl<SyncStateClass>::LockExclusive() {
  u64 old_state;
  for (auto counter = 0;; counter++) {
    old_state = StateAndVersion();
    if (TryLockExclusive(old_state)) { break; }
    AsmYield(counter);
  }
}

template <class SyncStateClass>
void HybridLatchImpl<SyncStateClass>::UnlockExclusive() {
  assert(IsExclusivelyLatched(StateAndVersion()));
  state_and_version_.store(NextVersionNewState(state_and_version_.load(), SyncStateClass::UNLOCKED),
                           std::memory_order_release);
}

template <class SyncStateClass>
void HybridLatchImpl<SyncStateClass>::DowngradeLock() {
  assert(IsExclusivelyLatched(StateAndVersion()));
  // 1 here means 1 reader
  state_and_version_.store(NextVersionNewState(state_and_version_.load(), 1), std::memory_order_release);
}

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::TryLockShared(u64 old_state_w_version) -> bool {
  u64 state = LockState(old_state_w_version);

  // Reach maximum number of readers
  if (state < SyncStateClass::MAX_SHARED) {
    return state_and_version_.compare_exchange_strong(old_state_w_version,
                                                      SameVersionNewState(old_state_w_version, state + 1));
  }

  return false;
}

template <class SyncStateClass>
void HybridLatchImpl<SyncStateClass>::LockShared() {
  u64 old_state;
  for (auto counter = 0;; counter++) {
    old_state = StateAndVersion();
    if (TryLockShared(old_state)) { break; }
    AsmYield(counter);
  }
}

template <class SyncStateClass>
auto HybridLatchImpl<SyncStateClass>::UpgradeLock(u64 old_state_w_version) -> bool {
  if (LockState(old_state_w_version) != 1) {
    // Only upgrade from shared -> exclusive if there is one reader
    return false;
  }
  return state_and_version_.compare_exchange_weak(old_state_w_version,
                                                  SameVersionNewState(old_state_w_version, SyncStateClass::EXCLUSIVE));
}

template <class SyncStateClass>
void HybridLatchImpl<SyncStateClass>::ForceUpgradeLock() {
  u64 old_state;
  for (auto counter = 0;; counter++) {
    old_state = StateAndVersion();
    if (UpgradeLock(old_state)) { break; }
    AsmYield(counter);
  }
}

template <class SyncStateClass>
void HybridLatchImpl<SyncStateClass>::UnlockShared() {
  while (true) {
    auto old_state_w_version = state_and_version_.load();
    auto old_state           = LockState(old_state_w_version);
    assert(IsSharedLatched(old_state_w_version));
    if (state_and_version_.compare_exchange_weak(old_state_w_version,
                                                 SameVersionNewState(old_state_w_version, old_state - 1))) {
      break;
    }
  }
}

template class HybridLatchImpl<HybridLatchMode>;
template class HybridLatchImpl<PageStateMode>;

}  // namespace leanstore::sync