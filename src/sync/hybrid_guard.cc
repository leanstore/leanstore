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
    default: break;
  }
}

HybridGuard::HybridGuard(HybridGuard &&other) noexcept
    : latch_(other.latch_), mode_(other.mode_), state_(other.state_) {
  other.mode_ = GuardMode::MOVED;
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
  if (mode_ != GuardMode::SHARED && mode_ != GuardMode::EXCLUSIVE) { return; }
  switch (mode_) {
    case GuardMode::SHARED: latch_->UnlockShared(); break;
    case GuardMode::EXCLUSIVE: latch_->UnlockExclusive(); break;
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

}  // namespace leanstore::sync