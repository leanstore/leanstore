#include "sync/hybrid_guard.h"
#include "common/exceptions.h"
#include "common/utils.h"

#include <shared_mutex>

namespace leanstore::sync {

HybridGuard::HybridGuard(HybridLatch *latch, GuardMode mode) : latch_(latch), mode_(mode) {
  switch (mode) {
    case GuardMode::OPTIMISTIC: OptimisticLock(); break;
    case GuardMode::SHARED: latch_->LockShared(); break;
    case GuardMode::EXCLUSIVE: latch_->LockExclusive(); break;
    case GuardMode::ADOPT_EXCLUSIVE: break;
    default: UnreachableCode();
  }
}

HybridGuard::HybridGuard(HybridGuard &&other) noexcept
    : latch_(other.latch_), mode_(other.mode_), state_(other.state_) {
  other.mode_ = GuardMode::MOVED;
}

auto HybridGuard::operator=(HybridGuard &&other) noexcept(false) -> HybridGuard & {
  if (this->mode_ == GuardMode::OPTIMISTIC) { this->ValidateOptimisticLock(); }
  this->latch_ = other.latch_;
  this->mode_  = other.mode_;
  this->state_ = other.state_;
  return *this;
}

HybridGuard::~HybridGuard() noexcept(false) {
  switch (mode_) {
    case GuardMode::OPTIMISTIC: ValidateOptimisticLock(); break;
    default: Unlock(); break;
  }
}

void HybridGuard::OptimisticLock() {
  if (mode_ != GuardMode::OPTIMISTIC) { return; }
  state_ = latch_->StateAndVersion().load();
  while (HybridLatch::LockState(state_) == HybridLatch::EXCLUSIVE) {
    AsmYield();
    state_ = latch_->StateAndVersion().load();
  }
}

void HybridGuard::Unlock() {
  if (mode_ != GuardMode::SHARED && mode_ != GuardMode::EXCLUSIVE && mode_ != GuardMode::ADOPT_EXCLUSIVE) { return; }
  switch (mode_) {
    case GuardMode::SHARED: latch_->UnlockShared(); break;
    case GuardMode::EXCLUSIVE:
    case GuardMode::ADOPT_EXCLUSIVE: latch_->UnlockExclusive(); break;
    default: break;
  }
  // Unlock successfully, moved to prevent extra check during guard destruction
  mode_ = GuardMode::MOVED;
}

void HybridGuard::ValidateOptimisticLock() {
  // This is guard for a shared lock, do nothing
  if (mode_ != GuardMode::OPTIMISTIC) { return; }
  // Done validation, moved to prevent extra check during guard destruction
  mode_            = GuardMode::MOVED;
  u64 latest_state = latch_->StateAndVersion().load();
  if (HybridLatch::LockState(latest_state) == HybridLatch::EXCLUSIVE ||
      HybridLatch::Version(latest_state) != HybridLatch::Version(state_)) {
    throw RestartException();
  }
}

void HybridGuard::UpgradeOptimisticToExclusive() {
  Ensure(mode_ == GuardMode::OPTIMISTIC);

  for (auto cnt = 0;; cnt++) {
    u64 latest_state = latch_->StateAndVersion().load();
    if (HybridLatch::LockState(latest_state) == HybridLatch::EXCLUSIVE ||
        HybridLatch::Version(latest_state) != HybridLatch::Version(state_)) {
      mode_ = GuardMode::MOVED;  // Prevent one additional validation
      throw RestartException();
    }

    u64 state = HybridLatch::LockState(latest_state);
    if (state == HybridLatch::UNLOCKED) {
      if (latch_->TryLockExclusive(latest_state)) {
        this->mode_  = GuardMode::EXCLUSIVE;
        this->state_ = HybridLatch::SameVersionNewState(latest_state, HybridLatch::EXCLUSIVE);
        break;
      }
    }
    AsmYield(cnt);
  }
}

}  // namespace leanstore::sync