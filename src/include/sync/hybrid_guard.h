#pragma once

#include "sync/hybrid_latch.h"

#include "gtest/gtest_prod.h"

namespace leanstore::sync {

enum class GuardMode { OPTIMISTIC, SHARED, EXCLUSIVE, MOVED };

class HybridGuard {
 public:
  HybridGuard(HybridLatch *latch, GuardMode mode);
  HybridGuard(HybridGuard &&other) noexcept;
  HybridGuard(const HybridGuard &) = delete;  // No COPY constructor
  ~HybridGuard() noexcept(false);

  void OptimisticLock();
  void Unlock();
  void ValidateOptimisticLock();

 private:
  FRIEND_TEST(TestGuard, UpgradeSharedToExclusive);

  HybridLatch *latch_;
  GuardMode mode_;
  u64 state_{0};
};

}  // namespace leanstore::sync