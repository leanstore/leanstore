#pragma once

#include "common/typedefs.h"

#include "gtest/gtest_prod.h"

#include <atomic>

namespace leanstore::sync {

class HybridGuard;

class HybridLatchMode {
 public:
  static constexpr u64 UNLOCKED   = 0;
  static constexpr u64 MAX_SHARED = 254;  // # share-lock holders (1 -> MAX_SHARED)
  static constexpr u64 EXCLUSIVE  = 255;
};

/**
 * All Classes which used to replace SyncStateClass
 *  should have the following states mandatory:
 *   - UNLOCKED
 *   - MAX_SHARED
 *   - EXCLUSIVE
 *
 * Additional states should be placed after EXCLUSIVE, e.g. buffer::PageStateMode
 * The size of those states should be not bigger than 8 bytes
 */
template <class SyncStateClass>
class HybridLatchImpl : public SyncStateClass {
 public:
  static constexpr u64 VERSION_MASK = (static_cast<u64>(1) << 56) - 1;

  HybridLatchImpl();
  ~HybridLatchImpl()                      = default;
  auto operator=(const HybridLatchImpl &) = delete;  // No COPY constructor
  auto operator=(HybridLatchImpl &&)      = delete;  // No MOVE constructor

  // State public utilities
  static auto LockState(u64 v) -> u64;
  static auto Version(u64 v) -> u64;
  auto LockState() -> u64;
  auto Version() -> u64;
  auto StateAndVersion() -> std::atomic<u64> &;

  // Lock utilities
  auto TryLockExclusive() -> bool;
  auto TryLockExclusive(u64 old_state_w_version) -> bool;
  void LockExclusive();
  void UnlockExclusive();
  void DowngradeLock();
  auto TryLockShared(u64 old_state_w_version) -> bool;
  void LockShared();
  auto UpgradeLock(u64 old_state_w_version) -> bool;
  void ForceUpgradeLock();
  void UnlockShared();

 protected:
  friend class HybridGuard;

  // Assertion utilities
  auto IsExclusivelyLatched(u64 v) -> bool { return LockState(v) == SyncStateClass::EXCLUSIVE; }

  auto IsSharedLatched(u64 v) -> bool {
    auto curr = LockState(v);
    return (SyncStateClass::UNLOCKED < curr) && (curr < SyncStateClass::EXCLUSIVE);
  }

  // State & Version utilities
  static auto SameVersionNewState(u64 old_state_and_version, u64 new_state) -> u64 {
    return ((old_state_and_version << 8) >> 8) | (new_state << 56);
  }

  static auto NextVersionNewState(u64 old_state_and_version, u64 new_state) -> u64 {
    return (((old_state_and_version << 8) >> 8) + 1) | (new_state << 56);
  }

  // the most-significant-8-bits is to store the state info
  //  (UNLOCKED / MAX_SHARED / LOCKED)
  // the rest (56 bits) is to store the version info
  std::atomic<u64> state_and_version_;
};

using HybridLatch = HybridLatchImpl<HybridLatchMode>;

}  // namespace leanstore::sync