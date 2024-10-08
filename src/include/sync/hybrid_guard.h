#pragma once

#include "sync/hybrid_latch.h"

#include "gtest/gtest_prod.h"

namespace leanstore::sync {

enum class GuardMode : u8 { OPTIMISTIC, SHARED, EXCLUSIVE, ADOPT_EXCLUSIVE, MOVED };

class HybridGuard {
 public:
  HybridGuard(HybridLatch *latch, GuardMode mode);
  HybridGuard(HybridGuard &&other) noexcept;
  auto operator=(HybridGuard &&other) noexcept(false) -> HybridGuard &;
  ~HybridGuard() noexcept(false);

  void OptimisticLock();
  void Unlock();
  void ValidateOptimisticLock();
  void UpgradeOptimisticToExclusive();

 private:
  HybridLatch *latch_;
  GuardMode mode_;
  u64 state_{0};
};

}  // namespace leanstore::sync