#include "sync/range_lock.h"
#include "common/exceptions.h"
#include "common/rand.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "sync/hybrid_guard.h"

#include <algorithm>

namespace leanstore::sync {

RangeLock::RangeLock(u64 no_threads)
    : root_(AllocNode(0, 0, MAX_LEVEL)), tail_(AllocNode(MAX_VALUE, MAX_VALUE, MAX_LEVEL)), epoch_(no_threads) {
  for (auto i = 0; i < MAX_LEVEL; i++) { root_->forward[i] = tail_; }
}

RangeLock::~RangeLock() {
  /* Free skip list */
  auto p = root_;
  do {
    auto q = p->forward[0];
    free(p);
    p = q;
  } while (p != nullptr);
}

auto RangeLock::AllocNode(u64 start, u64 end, u8 level) -> SkipListNode * {
  auto ptr   = reinterpret_cast<SkipListNode *>(malloc(sizeof(SkipListNode) + level * sizeof(SkipListNode *)));
  ptr->start = start;
  ptr->end   = end;
  for (auto i = 0; i < level; i++) { ptr->forward[i] = nullptr; }
  return ptr;
}

/**
 * Per every level, find the predecessor of node whose start > key
 * i.e. the node whose start is the largest possible and start <= key
 */
void RangeLock::FindNodes(u64 key, SkipListNode **out_nodes) {
  SkipListNode *q;
  auto p = root_;
  for (int k = static_cast<int>(level_); k >= 0; k--) {
    while (q = p->forward[k], q->start <= key) { p = q; }
    out_nodes[k] = p;
  }
}

void RangeLock::InsertRange(SkipListNode **nodes, u64 start, u64 end) {
  /* Adjust skip list level */
  auto lv = RandomGenerator::GetRand<u8>(1, MAX_LEVEL);
  if (lv > level_) {
    lv        = ++level_;
    nodes[lv] = root_;
  }

  /* Insert start and end */
  auto q = AllocNode(start, end, lv);
  for (auto k = 0; k < lv; k++) {
    auto p        = nodes[k];
    q->forward[k] = p->forward[k];
    p->forward[k] = q;
  }
}

// -------------------------------------------------------------------------------------

auto RangeLock::TryLockRange(u64 start, u64 len) -> bool {
  Ensure(start > 0);
  auto end = start + len - 1;
  SkipListNode *nodes[MAX_LEVEL];

  while (true) {
    try {
      EpochGuard ep(&(epoch_.local_epoch[worker_thread_id]), epoch_.global_epoch);

      /* Find all the nodes before insertion */
      HybridGuard guard(&latch_, GuardMode::OPTIMISTIC);
      FindNodes(end, nodes);
      if (nodes[0]->end >= start) { return false; }

      /* Acquire X lock before actual insertion */
      guard.UpgradeOptimisticToExclusive();
      InsertRange(nodes, start, end);
      return true;
    } catch (const sync::RestartException &) {}
  }

  epoch_.EpochOperation(worker_thread_id);
}

auto RangeLock::TestRange(u64 start, u64 len) -> bool {
  Ensure(start > 0);
  auto end = start + len - 1;
  SkipListNode *nodes[MAX_LEVEL];

  while (true) {
    try {
      EpochGuard ep(&(epoch_.local_epoch[worker_thread_id]), epoch_.global_epoch);
      HybridGuard guard(&latch_, GuardMode::OPTIMISTIC);
      FindNodes(end, nodes);
      return (nodes[0]->end < start);
    } catch (const sync::RestartException &) {}
  }
}

void RangeLock::UnlockRange(u64 start) {
  Ensure(start > 0);
  SkipListNode *q;
  SkipListNode *preds[MAX_LEVEL];

  while (true) {
    try {
      /* Find the predecessors of the to-be-deleted node */
      EpochGuard ep(&(epoch_.local_epoch[worker_thread_id]), epoch_.global_epoch);
      HybridGuard guard(&latch_, GuardMode::OPTIMISTIC);
      auto p = root_;
      for (int k = static_cast<int>(level_); k >= 0; k--) {
        while (q = p->forward[k], q->start < start) { p = q; }
        preds[k] = p;
      }
      q = preds[0]->forward[0];

      /* Delete the node */
      guard.UpgradeOptimisticToExclusive();
      Ensure(q->start == start);
      for (auto k = 0; k <= level_ && (p = preds[k])->forward[k] == q; k++) { p->forward[k] = q->forward[k]; }
      epoch_.DeferFreePointer(worker_thread_id, q);

      /* Adjust skip list level */
      while (root_->forward[level_] == nullptr && level_ > 0) { level_--; }
      return;
    } catch (const sync::RestartException &) {}
  }

  epoch_.EpochOperation(worker_thread_id);
}

}  // namespace leanstore::sync