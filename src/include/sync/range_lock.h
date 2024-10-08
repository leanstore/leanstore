#pragma once

#include "common/constants.h"
#include "sync/epoch_handler.h"
#include "sync/hybrid_latch.h"

#include <array>
#include <deque>
#include <memory>
#include <vector>

namespace leanstore::sync {

struct SkipListNode {
  u64 start;
  u64 end;
  SkipListNode *forward[];
};

/**
 * @brief Range lock implementation, backed by a B-Tree
 */
class RangeLock {
 public:
  static constexpr u8 MAX_LEVEL  = 32;
  static constexpr u64 MAX_VALUE = ~0ULL;

  explicit RangeLock(u64 no_threads);
  ~RangeLock();

  auto TryLockRange(u64 start, u64 len) -> bool;
  auto TestRange(u64 start, u64 len) -> bool;
  void UnlockRange(u64 start);

 private:
  auto AllocNode(u64 start, u64 end, u8 level) -> SkipListNode *;
  void FindNodes(u64 key, SkipListNode **out_nodes);
  void InsertRange(SkipListNode **nodes, u64 start, u64 end);

  /* Skip list operation */
  HybridLatch latch_;
  SkipListNode *root_;
  SkipListNode *tail_;
  u8 level_{0};

  /* Epoch management */
  EpochHandler epoch_;
};

}  // namespace leanstore::sync