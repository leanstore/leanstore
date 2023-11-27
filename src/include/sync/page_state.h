#pragma once

#include "common/typedefs.h"
#include "sync/hybrid_latch.h"

#include <atomic>
#include <string>

namespace leanstore::sync {

class PageStateMode {
 public:
  static constexpr u64 UNLOCKED   = 0;
  static constexpr u64 MAX_SHARED = 252;  // # share-lock holders (1 -> MAX_SHARED)
  static constexpr u64 EXCLUSIVE  = 253;
  static constexpr u64 MARKED     = 254;  // for Clock replacement policy
  static constexpr u64 EVICTED    = 255;
};

class PageState : public HybridLatchImpl<PageStateMode> {
 public:
  PageState()                       = default;
  ~PageState()                      = default;
  auto operator=(const PageState &) = delete;  // No COPY constructor
  auto operator=(PageState &&)      = delete;  // No MOVE constructor

  auto ToString() -> std::string;
  void Init();

  void UnlockExclusiveAndEvict();
  void UnlockExclusiveAndMark();
  auto TryLockShared(u64 old_state_w_version) -> bool;
  auto TryMark(u64 old_state_w_version) -> bool;
  auto Unmark(u64 old_state_w_version, u64 &new_state) -> bool;
};

}  // namespace leanstore::sync
