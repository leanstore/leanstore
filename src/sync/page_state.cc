#include "sync/page_state.h"
#include "sync/hybrid_latch.h"

#include <cassert>

namespace leanstore::sync {

void PageState::Init() {
  state_and_version_.store(SameVersionNewState(0, PageState::EVICTED), std::memory_order_release);
}

auto PageState::ToString() -> std::string {
  auto v             = state_and_version_.load();
  std::string result = std::string("[State: ") + std::to_string(PageState::LockState(v)) + std::string(", Version: ") +
                       std::to_string(PageState::Version(v)) + std::string("]");
  return result;
}

void PageState::UnlockExclusiveAndEvict() {
  assert(IsExclusivelyLatched(StateAndVersion()));
  state_and_version_.store(NextVersionNewState(state_and_version_.load(), PageState::EVICTED),
                           std::memory_order_release);
}

void PageState::UnlockExclusiveAndMark() {
  assert(IsExclusivelyLatched(StateAndVersion()));
  state_and_version_.store(NextVersionNewState(state_and_version_.load(), PageState::MARKED),
                           std::memory_order_release);
}

auto PageState::TryLockShared(u64 old_state_w_version) -> bool {
  u64 state = LockState(old_state_w_version);
  if (state < PageState::MAX_SHARED) {
    return state_and_version_.compare_exchange_strong(old_state_w_version,
                                                      SameVersionNewState(old_state_w_version, state + 1));
  }
  if (state == PageState::MARKED) {
    return state_and_version_.compare_exchange_strong(old_state_w_version, SameVersionNewState(old_state_w_version, 1));
  }

  return false;
}

auto PageState::TryMark(u64 old_state_w_version) -> bool {
  assert(LockState(old_state_w_version) == PageState::UNLOCKED);
  return state_and_version_.compare_exchange_strong(old_state_w_version,
                                                    SameVersionNewState(old_state_w_version, PageState::MARKED));
}

auto PageState::Unmark(u64 old_state_w_version, u64 &new_state) -> bool {
  assert(LockState(old_state_w_version) == PageState::MARKED);
  new_state = SameVersionNewState(old_state_w_version, PageState::UNLOCKED);
  return state_and_version_.compare_exchange_weak(old_state_w_version, new_state);
}

}  // namespace leanstore::sync