#include "BTreeVS.hpp"

#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
BTree::BTree() {}
// -------------------------------------------------------------------------------------
void BTree::create(DTID dtid, BufferFrame* meta_bf)
{
   auto root_write_guard_h = HybridPageGuard<BTreeNode>(dtid);
   auto root_write_guard = ExclusivePageGuard<BTreeNode>(std::move(root_write_guard_h));
   root_write_guard.init(true);
   // -------------------------------------------------------------------------------------
   this->meta_node_bf = meta_bf;
   HybridPageGuard<BTreeNode> meta_guard(meta_bf);
   ExclusivePageGuard meta_page(std::move(meta_guard));
   meta_page->upper = root_write_guard.bf();  // HACK: use upper of meta node as a swip to the storage root
   this->dt_id = dtid;
}
// -------------------------------------------------------------------------------------
bool BTree::lookupOne(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump<OP_TYPE::POINT_READ>(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->sanityCheck(key, key_length);
            leaf.recheck_done();
            if (sanity_check_result != 0) {
               cout << leaf->count << endl;
            }
            ensure(sanity_check_result == 0);
         }
         // -------------------------------------------------------------------------------------
         s16 pos = leaf->lowerBound<true>(key, key_length);
         if (pos != -1) {
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck_done();
            jumpmu_return true;
         } else {
            leaf.recheck_done();
            raise(SIGTRAP);
            jumpmu_return false;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::rangeScanAsc(u8* start_key, u16 key_length, std::function<bool(u8* key, u8* payload, u16 payload_length)> callback, function<void()> undo)
{
   volatile u32 mask = 1;
   u8* volatile next_key = start_key;
   volatile u16 next_key_length = key_length;
   volatile bool is_heap_freed = true;  // because at first we reuse the start_key
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         while (true) {
            findLeafCanJump<OP_TYPE::SCAN>(leaf, next_key, next_key_length);
            SharedPageGuard s_leaf(std::move(leaf));
            // -------------------------------------------------------------------------------------
            if (s_leaf->count == 0) {
               jumpmu_return;
            }
            s16 cur;
            if (next_key == start_key) {
               cur = s_leaf->lowerBound<false>(start_key, key_length);
            } else {
               cur = 0;
            }
            // -------------------------------------------------------------------------------------
            u16 prefix_length = s_leaf->prefix_length;
            u8 key[PAGE_SIZE];  // TODO
            s_leaf->copyPrefix(key);
            // -------------------------------------------------------------------------------------
            while (cur < s_leaf->count) {
               u16 payload_length = s_leaf->getPayloadLength(cur);
               u8* payload = s_leaf->getPayload(cur);
               s_leaf->copyKeyWithoutPrefix(cur, key + prefix_length);
               if (!callback(key, payload, payload_length)) {
                  if (!is_heap_freed) {
                     delete[] next_key;
                     is_heap_freed = true;
                  }
                  jumpmu_return;
               }
               cur++;
            }
            // -------------------------------------------------------------------------------------
            if (!is_heap_freed) {
               delete[] next_key;
               is_heap_freed = true;
            }
            if (s_leaf->isUpperFenceInfinity()) {
               jumpmu_return;
            }
            // -------------------------------------------------------------------------------------
            next_key_length = s_leaf->upper_fence.length + 1;
            next_key = new u8[next_key_length];
            is_heap_freed = false;
            memcpy(next_key, s_leaf->getUpperFenceKey(), s_leaf->upper_fence.length);
            next_key[next_key_length - 1] = 0;
         }
      }
      jumpmuCatch()
      {
         // Reset
         if (next_key == start_key) {
            assert(next_key_length == key_length);
            is_heap_freed = true;  // because at first we reuse the start_key
         } else {
            assert(is_heap_freed == false);
            delete[] next_key;
            next_key = start_key;
            next_key_length = key_length;
            is_heap_freed = true;
         }
         undo();
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::rangeScanDesc(u8* start_key,
                          u16 key_length,
                          std::function<bool(u8* key, u8* payload, u16 payload_length)> callback,
                          function<void()> undo)
{
   volatile u32 mask = 1;
   u8* volatile next_key = start_key;
   volatile u16 next_key_length = key_length;
   volatile bool is_heap_freed = true;  // because at first we reuse the start_key
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         while (true) {
            findLeafCanJump<OP_TYPE::SCAN>(leaf, next_key, next_key_length);
            SharedPageGuard s_leaf(std::move(leaf));
            // -------------------------------------------------------------------------------------
            if (s_leaf->count == 0) {
               jumpmu_return;
            }
            s16 cur;
            if (next_key == start_key) {
               cur = s_leaf->lowerBound<false>(start_key, key_length);
               if (s_leaf->lowerBound<true>(start_key, key_length) == -1) {
                  cur--;
               }
            } else {
               cur = s_leaf->count - 1;
            }
            // -------------------------------------------------------------------------------------
            u16 prefix_length = s_leaf->prefix_length;
            u8 key[PAGE_SIZE];  // TODO
            s_leaf->copyPrefix(key);
            // -------------------------------------------------------------------------------------
            while (cur >= 0) {
               u16 payload_length = s_leaf->getPayloadLength(cur);
               u8* payload = s_leaf->getPayload(cur);
               s_leaf->copyKeyWithoutPrefix(cur, key + prefix_length);
               if (!callback(key, payload, payload_length)) {
                  if (!is_heap_freed) {
                     delete[] next_key;
                     is_heap_freed = true;
                  }
                  jumpmu_return;
               }
               cur--;
            }
            // -------------------------------------------------------------------------------------
            if (!is_heap_freed) {
               delete[] next_key;
               is_heap_freed = true;
            }
            if (s_leaf->isLowerFenceInfinity()) {
               jumpmu_return;
            }
            // -------------------------------------------------------------------------------------
            next_key_length = s_leaf->lower_fence.length;
            next_key = new u8[next_key_length];
            is_heap_freed = false;
            memcpy(next_key, s_leaf->getLowerFenceKey(), s_leaf->lower_fence.length);
         }
      }
      jumpmuCatch()
      {
         {
            next_key = start_key;
            next_key_length = key_length;
            is_heap_freed = true;  // because at first we reuse the start_key
         }
         undo();
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
bool BTree::prefixMaxOne(u8* start_key, u16 start_key_length, function<void(const u8*, const u8*, u16)> payload_callback)
{
   volatile u32 mask = 1;
   u8 one_step_further_key[start_key_length];
   std::memcpy(one_step_further_key, start_key, start_key_length);
   if (++one_step_further_key[start_key_length - 1] == 0) {
      if (++one_step_further_key[start_key_length - 2] == 0) {
         ensure(false);
         // overflow is naively implemented
      }
   }
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump<OP_TYPE::POINT_READ>(leaf, one_step_further_key, start_key_length);
         const s16 cur = leaf->lowerBound<false>(one_step_further_key, start_key_length);
         if (cur > 0) {
            const s16 pos = cur - 1;
            const u16 payload_length = leaf->getPayloadLength(pos);
            const u8* payload = leaf->getPayload(pos);
            const u16 key_length = leaf->getFullKeyLen(pos);
            leaf.recheck();
            u8 key[key_length];
            leaf->copyFullKey(pos, key);
            payload_callback(key, payload, payload_length);
            leaf.recheck_done();
            jumpmu_return true;
         } else {
            if (leaf->lower_fence.length == 0) {
               jumpmu_return false;
            } else {
               const u16 lower_fence_key_length = leaf->lower_fence.length;
               u8 lower_fence_key[lower_fence_key_length];
               leaf.recheck();
               std::memcpy(lower_fence_key, leaf->getLowerFenceKey(), lower_fence_key_length);
               HybridPageGuard<BTreeNode> prev;
               findLeafCanJump<OP_TYPE::POINT_READ>(prev, lower_fence_key, lower_fence_key_length);
               leaf.recheck_done();
               // -------------------------------------------------------------------------------------
               ensure(prev->count >= 1);
               const s16 pos = prev->count - 1;
               const u16 payload_length = prev->getPayloadLength(pos);
               const u8* payload = prev->getPayload(pos);
               const u16 key_length = prev->getFullKeyLen(pos);
               prev.recheck();
               u8 key[key_length];
               prev->copyFullKey(pos, key);
               payload_callback(key, payload, payload_length);
               prev.recheck_done();
               leaf.recheck_done();
               jumpmu_return true;
            }
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::insert(u8* key, u16 key_length, u64 value_length, u8* value)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   volatile u32 mask = 1;
   volatile u32 local_restarts_counter = 0;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> c_guard;
         findLeafCanJump<OP_TYPE::POINT_INSERT>(c_guard, key, key_length);
         // -------------------------------------------------------------------------------------
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         if (c_x_guard->prepareInsert(key, key_length, ValueType(reinterpret_cast<BufferFrame*>(value_length)))) {
            c_x_guard->insert(key, key_length, ValueType(reinterpret_cast<BufferFrame*>(value_length)), value);
            if (FLAGS_wal) {
               auto wal_entry = c_x_guard.reserveWALEntry<WALInsert>(key_length + value_length);
               wal_entry->type = WAL_LOG_TYPE::WALInsert;
               wal_entry->key_length = key_length;
               wal_entry->value_length = value_length;
               std::memcpy(wal_entry->payload, key, key_length);
               std::memcpy(wal_entry->payload + key_length, value, value_length);
               wal_entry.submit();
            }
            jumpmu_return;
         }
         // -------------------------------------------------------------------------------------
         // Release lock
         c_guard = std::move(c_x_guard);
         c_guard.kill();
         // -------------------------------------------------------------------------------------
         trySplit(*c_guard.bf);
         // -------------------------------------------------------------------------------------
         jumpmu_continue;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_structural_change[dt_id]++;
         local_restarts_counter++;
      }
   }
}
// -------------------------------------------------------------------------------------
bool BTree::tryBalanceRight(HybridPageGuard<BTreeNode>& parent, HybridPageGuard<BTreeNode>& left, s16 l_pos)
{
   if (isMetaNode(parent) || l_pos + 1 >= parent->count) {
      return false;
   }
   HybridPageGuard<BTreeNode> right = HybridPageGuard(parent, parent->getChild(l_pos + 1));
   // -------------------------------------------------------------------------------------
   // Rebalance: move key/value from end of left to the beginning of right
   const u32 total_free_space = left->freeSpaceAfterCompaction() + right->freeSpaceAfterCompaction();
   const u32 r_target_free_space = total_free_space / 2;
   BTreeNode tmp(true);
   tmp.setFences(left->getLowerFenceKey(), left->lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
   ensure(tmp.prefix_length <= right->prefix_length);
   const u32 worst_case_amplification_per_key = 2 + right->prefix_length - tmp.prefix_length;
   // -------------------------------------------------------------------------------------
   s64 r_free_space = right->freeSpaceAfterCompaction() - 512;
   r_free_space -= (worst_case_amplification_per_key * right->count);
   if (r_free_space <= 0)
      return false;
   s16 left_boundary = -1;  // exclusive
   for (s16 s_i = left->count - 1; s_i > 0; s_i--) {
      r_free_space -= left->spaceUsedBySlot(s_i) + (worst_case_amplification_per_key);
      const u16 new_right_lf_key_length = left->getFullKeyLen(s_i);
      if ((r_free_space - ((right->lower_fence.length < new_right_lf_key_length) ? (new_right_lf_key_length - right->lower_fence.length) : 0)) >
          r_target_free_space) {
         left_boundary = s_i - 1;
      } else {
         break;
      }
   }
   // -------------------------------------------------------------------------------------
   if (left_boundary == -1) {
      return false;
   }
   // -------------------------------------------------------------------------------------
   // temporary hack
   if (left->getFullKeyLen(left_boundary) > left->upper_fence.length) {
      return false;
   }
   // -------------------------------------------------------------------------------------
   u16 new_left_uf_length = left->getFullKeyLen(left_boundary);
   ensure(new_left_uf_length > 0);
   u8 new_left_uf_key[new_left_uf_length];
   left->copyFullKey(left_boundary, new_left_uf_key);
   // -------------------------------------------------------------------------------------
   const u16 old_left_sep_space = parent->spaceUsedBySlot(l_pos);
   const u16 new_left_sep_space = parent->spaceNeeded(new_left_uf_length, left.swip());
   if (new_left_sep_space > old_left_sep_space) {
      if (!parent->hasEnoughSpaceFor(new_left_sep_space - old_left_sep_space))
         return false;
   }
   // -------------------------------------------------------------------------------------
   ExclusivePageGuard<BTreeNode> x_parent = std::move(parent);
   ExclusivePageGuard<BTreeNode> x_left = std::move(left);
   ExclusivePageGuard<BTreeNode> x_right = std::move(right);
   // -------------------------------------------------------------------------------------
   const u16 copy_from_count = left->count - (left_boundary + 1);
   // -------------------------------------------------------------------------------------
   {
      tmp = BTreeNode(true);
      // Right node
      tmp.setFences(new_left_uf_key, new_left_uf_length, x_right->getUpperFenceKey(), x_right->upper_fence.length);
      // -------------------------------------------------------------------------------------
      x_left->copyKeyValueRange(&tmp, 0, left_boundary + 1, copy_from_count);
      x_right->copyKeyValueRange(&tmp, copy_from_count, 0, x_right->count);
      memcpy(reinterpret_cast<u8*>(x_right.ptr()), &tmp, sizeof(BTreeNode));
      x_right->makeHint();
      // -------------------------------------------------------------------------------------
      // Nothing to do for the right node's separator
   }
   {
      tmp = BTreeNode(true);
      tmp.setFences(x_left->getLowerFenceKey(), x_left->lower_fence.length, new_left_uf_key, new_left_uf_length);
      // -------------------------------------------------------------------------------------
      x_left->copyKeyValueRange(&tmp, 0, 0, x_left->count - copy_from_count);
      ensure(x_left->freeSpaceAfterCompaction() <= tmp.freeSpaceAfterCompaction());
      memcpy(reinterpret_cast<u8*>(left.ptr()), &tmp, sizeof(BTreeNode));
      x_left->makeHint();
      // -------------------------------------------------------------------------------------
   }
   {
      x_parent->removeSlot(l_pos);
      ensure(x_parent->prepareInsert(x_left->getUpperFenceKey(), x_left->upper_fence.length, left.swip()));
      x_parent->insert(x_left->getUpperFenceKey(), x_left->upper_fence.length, left.swip());
   }
   // -------------------------------------------------------------------------------------
   return true;
}
// -------------------------------------------------------------------------------------
void BTree::trySplit(BufferFrame& to_split, s16 favored_split_pos)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   auto parent_handler = findParent(this, to_split);
   HybridPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
   HybridPageGuard<BTreeNode> c_guard = HybridPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
   if (c_guard->count <= 2)
      return;
   // -------------------------------------------------------------------------------------
   BTreeNode::SeparatorInfo sep_info;
   if (favored_split_pos < 0 || favored_split_pos >= c_guard->count - 1) {
      if (FLAGS_bulk_insert) {
         favored_split_pos = c_guard->count - 2;
         sep_info = BTreeNode::SeparatorInfo{c_guard->getFullKeyLen(favored_split_pos), static_cast<u16>(favored_split_pos), false};
      } else {
         sep_info = c_guard->findSep();
      }
   } else {
      // Split on a specified position, used by contention management
      sep_info = BTreeNode::SeparatorInfo{c_guard->getFullKeyLen(favored_split_pos), static_cast<u16>(favored_split_pos), false};
   }
   u8 sep_key[sep_info.length];
   if (isMetaNode(p_guard)) {  // root split
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      assert(height == 1 || !c_x_guard->is_leaf);
      // -------------------------------------------------------------------------------------
      // create new root
      auto new_root_h = HybridPageGuard<BTreeNode>(dt_id, false);
      auto new_root = ExclusivePageGuard<BTreeNode>(std::move(new_root_h));
      auto new_left_node_h = HybridPageGuard<BTreeNode>(dt_id);
      auto new_left_node = ExclusivePageGuard<BTreeNode>(std::move(new_left_node_h));
      // -------------------------------------------------------------------------------------
      auto exec = [&]() {
         new_root.keepAlive();
         new_root.init(false);
         new_root->upper = c_x_guard.bf();
         p_x_guard->upper = new_root.bf();
         // -------------------------------------------------------------------------------------
         new_left_node.init(c_x_guard->is_leaf);
         c_x_guard->getSep(sep_key, sep_info);
         // -------------------------------------------------------------------------------------
         c_x_guard->split(new_root, new_left_node, sep_info.slot, sep_key, sep_info.length);
      };
      if (FLAGS_wal) {
         WALLogicalSplit logical_split_entry;
         logical_split_entry.right_pid = c_x_guard.bf()->header.pid;
         logical_split_entry.parent_pid = new_root.bf()->header.pid;
         logical_split_entry.left_pid = new_left_node.bf()->header.pid;
         // -------------------------------------------------------------------------------------
         auto current_right_wal = c_x_guard.reserveWALEntry<WALLogicalSplit>(0);
         *current_right_wal = logical_split_entry;
         current_right_wal.submit();
         // -------------------------------------------------------------------------------------
         exec();
         // -------------------------------------------------------------------------------------
         auto root_wal = new_root.reserveWALEntry<WALLogicalSplit>(0);
         *root_wal = logical_split_entry;
         root_wal.submit();
         // -------------------------------------------------------------------------------------
         auto left_wal = new_left_node.reserveWALEntry<WALLogicalSplit>(0);
         *left_wal = logical_split_entry;
         left_wal.submit();
      } else {
         exec();
      }
      // -------------------------------------------------------------------------------------
      height++;
      return;
   } else {
      // Parent is not root
      u16 spaced_need_for_separator = BTreeNode::spaceNeededAsInner(sep_info.length, p_guard->prefix_length);
      if (p_guard->hasEnoughSpaceFor(spaced_need_for_separator)) {  // Is there enough space in the parent
                                                                    // for the separator?
         auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         p_x_guard->requestSpaceFor(spaced_need_for_separator);
         assert(meta_node_bf != p_x_guard.bf());
         assert(!p_x_guard->is_leaf);
         // -------------------------------------------------------------------------------------
         auto new_left_node_h = HybridPageGuard<BTreeNode>(dt_id);
         auto new_left_node = ExclusivePageGuard<BTreeNode>(std::move(new_left_node_h));
         // -------------------------------------------------------------------------------------
         auto exec = [&]() {
            new_left_node.init(c_x_guard->is_leaf);
            c_x_guard->getSep(sep_key, sep_info);
            c_x_guard->split(p_x_guard, new_left_node, sep_info.slot, sep_key, sep_info.length);
         };
         // -------------------------------------------------------------------------------------
         if (FLAGS_wal) {
            WALLogicalSplit logical_split_entry;
            logical_split_entry.right_pid = c_x_guard.bf()->header.pid;
            logical_split_entry.parent_pid = p_x_guard.bf()->header.pid;
            logical_split_entry.left_pid = new_left_node.bf()->header.pid;
            // -------------------------------------------------------------------------------------
            auto current_right_wal = c_x_guard.reserveWALEntry<WALLogicalSplit>(0);
            *current_right_wal = logical_split_entry;
            current_right_wal.submit();
            // -------------------------------------------------------------------------------------
            exec();
            // -------------------------------------------------------------------------------------
            auto parent_wal = p_x_guard.reserveWALEntry<WALLogicalSplit>(0);
            *parent_wal = logical_split_entry;
            parent_wal.submit();
            // -------------------------------------------------------------------------------------
            auto left_init_wal = new_left_node.reserveWALEntry<WALInitPage>(0);
            left_init_wal->dt_id = dt_id;
            left_init_wal.submit();
            auto left_wal = new_left_node.reserveWALEntry<WALLogicalSplit>(0);
            *left_wal = logical_split_entry;
            left_wal.submit();
         } else {
            exec();
         }
      } else {
         p_guard.kill();
         c_guard.kill();
         trySplit(*p_guard.bf);  // Must split parent head to make space for separator
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::updateSameSize(u8* key, u16 key_length, function<void(u8* payload, u16 payload_size)> callback, WALUpdateGenerator wal_update_generator)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         // -------------------------------------------------------------------------------------
         HybridPageGuard<BTreeNode> c_guard;
         findLeafCanJump<OP_TYPE::POINT_UPDATE>(c_guard, key, key_length);
         u32 local_restarts_counter = c_guard.hasFacedContention();  // current implementation uses the mutex
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         s16 pos = c_x_guard->lowerBound<true>(key, key_length);
         assert(pos != -1);
         u16 payload_length = c_x_guard->getPayloadLength(pos);
         // -------------------------------------------------------------------------------------
         if (FLAGS_wal) {
            // if it is a secondary index, then we can not use updateSameSize
            assert(wal_update_generator.entry_size > 0);
            // -------------------------------------------------------------------------------------
            auto wal_entry = c_x_guard.reserveWALEntry<WALUpdate>(key_length + wal_update_generator.entry_size);
            wal_entry->type = WAL_LOG_TYPE::WALUpdate;
            wal_entry->key_length = key_length;
            std::memcpy(wal_entry->payload, key, key_length);
            wal_update_generator.before(c_x_guard->getPayload(pos), wal_entry->payload + key_length);
            // The actual update by the client
            callback(c_x_guard->getPayload(pos), payload_length);
            wal_update_generator.after(c_x_guard->getPayload(pos), wal_entry->payload + key_length);
            wal_entry.submit();
         } else {
            callback(c_x_guard->getPayload(pos), payload_length);
         }
         // -------------------------------------------------------------------------------------
         if (FLAGS_contention_split && local_restarts_counter > 0) {
            const u64 random_number = utils::RandomGenerator::getRandU64();
            if ((random_number & ((1ull << FLAGS_cm_update_on) - 1)) == 0) {
               s64 last_modified_pos = c_x_guard.bf()->header.contention_tracker.last_modified_pos;
               c_x_guard.bf()->header.contention_tracker.last_modified_pos = pos;
               c_x_guard.bf()->header.contention_tracker.restarts_counter += local_restarts_counter;
               c_x_guard.bf()->header.contention_tracker.access_counter++;
               if ((random_number & ((1ull << FLAGS_cm_period) - 1)) == 0) {
                  const u64 current_restarts_counter = c_x_guard.bf()->header.contention_tracker.restarts_counter;
                  const u64 current_access_counter = c_x_guard.bf()->header.contention_tracker.access_counter;
                  const u64 normalized_restarts = 100.0 * current_restarts_counter / current_access_counter;
                  c_x_guard.bf()->header.contention_tracker.restarts_counter = 0;
                  c_x_guard.bf()->header.contention_tracker.access_counter = 0;
                  // -------------------------------------------------------------------------------------
                  if (last_modified_pos != pos && normalized_restarts >= FLAGS_cm_slowpath_threshold && c_x_guard->count > 2) {
                     s16 split_pos = std::min<s16>(last_modified_pos, pos);
                     c_guard = std::move(c_x_guard);
                     c_guard.kill();
                     jumpmuTry()
                     {
                        trySplit(*c_guard.bf, split_pos);
                        WorkerCounters::myCounters().contention_split_succ_counter[dt_id]++;
                     }
                     jumpmuCatch() { WorkerCounters::myCounters().contention_split_fail_counter[dt_id]++; }
                  }
               }
            }
         } else {
            c_guard = std::move(c_x_guard);
         }
         jumpmu_return;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_update_same_size[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
// TODO:
void BTree::update(u8*, u16, u64, u8*)
{
   ensure(false);
}
// -------------------------------------------------------------------------------------
bool BTree::remove(u8* key, u16 key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> c_guard;
         findLeafCanJump<OP_TYPE::POINT_DELETE>(c_guard, key, key_length);
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         if (c_x_guard->remove(key, key_length)) {
            if (FLAGS_wal) {
               auto wal_entry = c_x_guard.reserveWALEntry<WALRemove>(key_length);
               wal_entry->type = WAL_LOG_TYPE::WALRemove;
               wal_entry->key_length = key_length;
               std::memcpy(wal_entry->payload, key, key_length);
               wal_entry.submit();
            }
         } else {
            jumpmu_return false;
         }
         if (c_x_guard->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
            c_guard = std::move(c_x_guard);
            c_guard.kill();
            jumpmuTry() { tryMerge(*c_guard.bf); }
            jumpmuCatch()
            {
               // nothing, it is fine not to merge
            }
         }
         jumpmu_return true;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_structural_change[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
bool BTree::tryMerge(BufferFrame& to_merge, bool swizzle_sibling)
{
   auto parent_handler = findParent(this, to_merge);
   HybridPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
   HybridPageGuard<BTreeNode> c_guard = HybridPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
   int pos = parent_handler.pos;
   if (isMetaNode(p_guard) || c_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
      p_guard.kill();
      c_guard.kill();
      return false;
   }
   // -------------------------------------------------------------------------------------
   if (pos >= p_guard->count) {
      // TODO: we do not merge the node if it is the upper swip of parent
      return false;
   }
   // -------------------------------------------------------------------------------------
   p_guard.recheck();
   c_guard.recheck();
   // -------------------------------------------------------------------------------------
   auto merge_left = [&]() {
      Swip<BTreeNode>& l_swip = p_guard->getChild(pos - 1);
      if (!swizzle_sibling && !l_swip.isHOT()) {
         return false;
      }
      auto l_guard = HybridPageGuard(p_guard, l_swip);
      if (l_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
         l_guard.kill();
         return false;
      }
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      auto l_x_guard = ExclusivePageGuard(std::move(l_guard));
      // -------------------------------------------------------------------------------------
      if (!l_x_guard->merge(pos - 1, p_x_guard, c_x_guard)) {
         p_guard = std::move(p_x_guard);
         c_guard = std::move(c_x_guard);
         l_guard = std::move(l_x_guard);
         return false;
      }
      l_x_guard.reclaim();
      // -------------------------------------------------------------------------------------
      p_guard = std::move(p_x_guard);
      c_guard = std::move(c_x_guard);
      return true;
   };
   auto merge_right = [&]() {
      Swip<BTreeNode>& r_swip = p_guard->getChild(pos + 1);
      if (!swizzle_sibling && !r_swip.isHOT()) {
         return false;
      }
      auto r_guard = HybridPageGuard(p_guard, r_swip);
      if (r_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
         r_guard.kill();
         return false;
      }
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      auto r_x_guard = ExclusivePageGuard(std::move(r_guard));
      // -------------------------------------------------------------------------------------
      assert(p_x_guard->getChild(pos).bfPtr() == c_x_guard.bf());
      if (!c_x_guard->merge(pos, p_x_guard, r_x_guard)) {
         p_guard = std::move(p_x_guard);
         c_guard = std::move(c_x_guard);
         r_guard = std::move(r_x_guard);
         return false;
      }
      c_x_guard.reclaim();
      // -------------------------------------------------------------------------------------
      p_guard = std::move(p_x_guard);
      r_guard = std::move(r_x_guard);
      return true;
   };
   // ATTENTION: don't use c_guard without making sure it was not reclaimed
   // -------------------------------------------------------------------------------------
   volatile bool merged_successfully = false;
   if (p_guard->count > 2) {
      if (pos > 0) {
         merged_successfully |= merge_left();
      }
      if (!merged_successfully && (pos + 1 < p_guard->count)) {
         merged_successfully |= merge_right();
      }
   }
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
      HybridPageGuard<BTreeNode> meta_guard(meta_node_bf);
      if (!isMetaNode(p_guard) && p_guard->freeSpaceAfterCompaction() >= BTreeNode::underFullSize) {
         tryMerge(*p_guard.bf, swizzle_sibling);
      }
   }
   jumpmuCatch() {}
   // -------------------------------------------------------------------------------------
   return merged_successfully;
}
// -------------------------------------------------------------------------------------
// ret: 0 did nothing, 1 full, 2 partial
s16 BTree::mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent,
                              s16 left_pos,
                              ExclusivePageGuard<BTreeNode>& from_left,
                              ExclusivePageGuard<BTreeNode>& to_right,
                              bool full_merge_or_nothing)
{
   // TODO: corner cases: new upper fence is larger than the older one.
   u32 space_upper_bound = from_left->mergeSpaceUpperBound(to_right);
   if (space_upper_bound <= EFFECTIVE_PAGE_SIZE) {  // Do a full merge TODO: threshold
      bool succ = from_left->merge(left_pos, parent, to_right);
      static_cast<void>(succ);
      assert(succ);
      from_left.reclaim();
      return 1;
   }
   if (full_merge_or_nothing)
      return 0;
   // -------------------------------------------------------------------------------------
   // Do a partial merge
   // Remove a key at a time from the merge and check if now it fits
   s16 till_slot_id = -1;
   for (s16 s_i = 0; s_i < from_left->count; s_i++) {
      space_upper_bound -= sizeof(BTreeNode::Slot) + sizeof(ValueType) + from_left->getKeyLen(s_i) + from_left->getPayloadLength(s_i);
      if (space_upper_bound + (from_left->getFullKeyLen(s_i) - to_right->lower_fence.length) < EFFECTIVE_PAGE_SIZE * 1.0) {
         till_slot_id = s_i + 1;
         break;
      }
   }
   if (!(till_slot_id != -1 && till_slot_id < (from_left->count - 1)))
      return 0;  // false

   assert((space_upper_bound + (from_left->getFullKeyLen(till_slot_id - 1) - to_right->lower_fence.length)) < EFFECTIVE_PAGE_SIZE * 1.0);
   assert(till_slot_id > 0);
   // -------------------------------------------------------------------------------------
   u16 copy_from_count = from_left->count - till_slot_id;
   // -------------------------------------------------------------------------------------
   u16 new_left_uf_length = from_left->getFullKeyLen(till_slot_id - 1);
   ensure(new_left_uf_length > 0);
   u8 new_left_uf_key[new_left_uf_length];
   from_left->copyFullKey(till_slot_id - 1, new_left_uf_key);
   // -------------------------------------------------------------------------------------
   if (!parent->prepareInsert(new_left_uf_key, new_left_uf_length, 0))
      return 0;  // false
   // -------------------------------------------------------------------------------------
   // cout << till_slot_id << '\t' << from_left->count << '\t' << to_right->count << endl;
   // -------------------------------------------------------------------------------------
   {
      BTreeNode tmp(true);
      tmp.setFences(new_left_uf_key, new_left_uf_length, to_right->getUpperFenceKey(), to_right->upper_fence.length);
      // -------------------------------------------------------------------------------------
      from_left->copyKeyValueRange(&tmp, 0, till_slot_id, copy_from_count);
      to_right->copyKeyValueRange(&tmp, copy_from_count, 0, to_right->count);
      memcpy(reinterpret_cast<u8*>(to_right.ptr()), &tmp, sizeof(BTreeNode));
      to_right->makeHint();
      // -------------------------------------------------------------------------------------
      // Nothing to do for the right node's separator
      assert(to_right->sanityCheck(new_left_uf_key, new_left_uf_length) == 1);
   }
   {
      BTreeNode tmp(true);
      tmp.setFences(from_left->getLowerFenceKey(), from_left->lower_fence.length, new_left_uf_key, new_left_uf_length);
      // -------------------------------------------------------------------------------------
      from_left->copyKeyValueRange(&tmp, 0, 0, from_left->count - copy_from_count);
      memcpy(reinterpret_cast<u8*>(from_left.ptr()), &tmp, sizeof(BTreeNode));
      from_left->makeHint();
      // -------------------------------------------------------------------------------------
      assert(from_left->sanityCheck(new_left_uf_key, new_left_uf_length) == 0);
      // -------------------------------------------------------------------------------------
      parent->removeSlot(left_pos);
      ensure(parent->prepareInsert(from_left->getUpperFenceKey(), from_left->upper_fence.length, from_left.swip()));
      parent->insert(from_left->getUpperFenceKey(), from_left->upper_fence.length, from_left.swip());
   }
   return 2;
}
// -------------------------------------------------------------------------------------
// returns true if it has exclusively locked anything
BTree::XMergeReturnCode BTree::XMerge(HybridPageGuard<BTreeNode>& p_guard, HybridPageGuard<BTreeNode>& c_guard, ParentSwipHandler& parent_handler)
{
   WorkerCounters::myCounters().dt_researchy[0][1]++;
   if (c_guard->fillFactorAfterCompaction() >= 0.9) {
      return XMergeReturnCode::NOTHING;
   }
   // -------------------------------------------------------------------------------------
   const u8 MAX_MERGE_PAGES = FLAGS_xmerge_k;
   s16 pos = parent_handler.pos;
   u8 pages_count = 1;
   s16 max_right;
   HybridPageGuard<BTreeNode> guards[MAX_MERGE_PAGES];
   bool fully_merged[MAX_MERGE_PAGES];
   // -------------------------------------------------------------------------------------
   guards[0] = std::move(c_guard);
   fully_merged[0] = false;
   double total_fill_factor = guards[0]->fillFactorAfterCompaction();
   // -------------------------------------------------------------------------------------
   // Handle upper swip instead of avoiding p_guard->count -1 swip
   if (isMetaNode(p_guard) || !guards[0]->is_leaf) {
      c_guard = std::move(guards[0]);
      return XMergeReturnCode::NOTHING;
   }
   for (max_right = pos + 1; (max_right - pos) < MAX_MERGE_PAGES && (max_right + 1) < p_guard->count; max_right++) {
      if (!p_guard->getChild(max_right).isHOT()) {
         c_guard = std::move(guards[0]);
         return XMergeReturnCode::NOTHING;
      }
      // -------------------------------------------------------------------------------------
      guards[max_right - pos] = HybridPageGuard<BTreeNode>(p_guard, p_guard->getChild(max_right));
      fully_merged[max_right - pos] = false;
      total_fill_factor += guards[max_right - pos]->fillFactorAfterCompaction();
      pages_count++;
      if ((pages_count - std::ceil(total_fill_factor)) >= (1)) {
         // we can probably save a page by merging all together so there is no need to look furhter
         break;
      }
   }
   if (((pages_count - std::ceil(total_fill_factor))) < (1)) {
      c_guard = std::move(guards[0]);
      return XMergeReturnCode::NOTHING;
   }
   // -------------------------------------------------------------------------------------
   ExclusivePageGuard<BTreeNode> p_x_guard = std::move(p_guard);
   // -------------------------------------------------------------------------------------
   XMergeReturnCode ret_code = XMergeReturnCode::PARTIAL_MERGE;
   s16 left_hand, right_hand, ret;
   while (true) {
      for (right_hand = max_right; right_hand > pos; right_hand--) {
         if (fully_merged[right_hand - pos]) {
            continue;
         } else {
            break;
         }
      }
      if (right_hand == pos)
         break;
      // -------------------------------------------------------------------------------------
      left_hand = right_hand - 1;
      // -------------------------------------------------------------------------------------
      {
         ExclusivePageGuard<BTreeNode> right_x_guard(std::move(guards[right_hand - pos]));
         ExclusivePageGuard<BTreeNode> left_x_guard(std::move(guards[left_hand - pos]));
         max_right = left_hand;
         ret = mergeLeftIntoRight(p_x_guard, left_hand, left_x_guard, right_x_guard, left_hand == pos);
         // we unlock only the left page, the right one should not be touched again
         if (ret == 1) {
            fully_merged[left_hand - pos] = true;
            WorkerCounters::myCounters().xmerge_full_counter[dt_id]++;
            ret_code = XMergeReturnCode::FULL_MERGE;
         } else if (ret == 2) {
            guards[left_hand - pos] = std::move(left_x_guard);
            WorkerCounters::myCounters().xmerge_partial_counter[dt_id]++;
         } else if (ret == 0) {
            break;
         } else {
            ensure(false);
         }
      }
      // -------------------------------------------------------------------------------------
   }
   if (c_guard.guard.state == GUARD_STATE::MOVED)
      c_guard = std::move(guards[0]);
   p_guard = std::move(p_x_guard);
   return ret_code;
}
// -------------------------------------------------------------------------------------
BTree::~BTree() {}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTree::getMeta()
{
   DTRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
                                    .find_parent = findParent,
                                    .check_space_utilization = checkSpaceUtilization,
                                    .checkpoint = checkpoint};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
// Called by buffer manager before eviction
// Returns true if the buffer manager has to restart and pick another buffer frame for eviction
// Attention: the guards here down the stack are not synchronized with the ones in the buffer frame manager stack frame
bool BTree::checkSpaceUtilization(void* btree_object, BufferFrame& bf, OptimisticGuard& o_guard, ParentSwipHandler& parent_handler)
{
   if (FLAGS_xmerge) {
      auto& btree = *reinterpret_cast<BTree*>(btree_object);
      HybridPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
      HybridPageGuard<BTreeNode> c_guard(o_guard.guard, &bf);
      XMergeReturnCode return_code = btree.XMerge(p_guard, c_guard, parent_handler);
      o_guard.guard = std::move(c_guard.guard);
      parent_handler.parent_guard = std::move(p_guard.guard);
      p_guard.kill();
      c_guard.kill();
      return (return_code != XMergeReturnCode::NOTHING);
   }
   return false;
}
// -------------------------------------------------------------------------------------
void BTree::checkpoint(void*, BufferFrame& bf, u8* dest)
{
   std::memcpy(dest, bf.page.dt, EFFECTIVE_PAGE_SIZE);
   auto node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
   auto dest_node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
   if (!node.is_leaf) {
      for (u64 t_i = 0; t_i < dest_node.count; t_i++) {
         if (!dest_node.getChild(t_i).isEVICTED()) {
            auto& bf = dest_node.getChild(t_i).bfRefAsHot();
            dest_node.getChild(t_i).evict(bf.header.pid);
         }
      }
      if (!dest_node.upper.isEVICTED()) {
         auto& bf = dest_node.upper.bfRefAsHot();
         dest_node.upper.evict(bf.header.pid);
      }
   }
}
// -------------------------------------------------------------------------------------
// TODO: Refactor
// Jump if any page on the path is already evicted
// Throws if the bf could not be found
struct ParentSwipHandler BTree::findParent(void* btree_object, BufferFrame& to_find)
{
   // Pre: bf is write locked TODO: but trySplit does not ex lock !
   auto& c_node = *reinterpret_cast<BTreeNode*>(to_find.page.dt);
   auto& btree = *reinterpret_cast<BTree*>(btree_object);
   // -------------------------------------------------------------------------------------
   HybridPageGuard<BTreeNode> p_guard(btree.meta_node_bf);
   u16 level = 0;
   // -------------------------------------------------------------------------------------
   Swip<BTreeNode>* c_swip = &p_guard->upper;
   if (btree.dt_id != to_find.page.dt_id || (!p_guard->upper.isHOT())) {
      jumpmu::jump();
   }
   // -------------------------------------------------------------------------------------
   const bool infinity = c_node.upper_fence.offset == 0;
   const u16 key_length = c_node.upper_fence.length;
   u8* key = c_node.getUpperFenceKey();
   // -------------------------------------------------------------------------------------
   // check if bf is the root node
   if (c_swip->bfPtrAsHot() == &to_find) {
      p_guard.recheck_done();
      return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(p_guard.guard), .parent_bf = btree.meta_node_bf};
   }
   // -------------------------------------------------------------------------------------
   HybridPageGuard c_guard(p_guard, p_guard->upper);  // the parent of the bf we are looking for (to_find)
   s16 pos = -1;
   auto search_condition = [&]() {
      if (infinity) {
         c_swip = &(c_guard->upper);
         pos = c_guard->count;
      } else {
         pos = c_guard->lowerBound<false>(key, key_length);
         if (pos == c_guard->count) {
            c_swip = &(c_guard->upper);
         } else {
            c_swip = &(c_guard->getChild(pos));
         }
      }
      return (c_swip->bfPtrAsHot() != &to_find);
   };
   while (!c_guard->is_leaf && search_condition()) {
      p_guard = std::move(c_guard);
      if (c_swip->isEVICTED()) {
         jumpmu::jump();
      }
      c_guard = HybridPageGuard(p_guard, c_swip->cast<BTreeNode>());
      level++;
   }
   p_guard.kill();
   const bool found = c_swip->bfPtrAsHot() == &to_find;
   c_guard.recheck_done();
   if (!found) {
      jumpmu::jump();
   }
   return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(c_guard.guard), .parent_bf = c_guard.bf, .pos = pos};
}
// -------------------------------------------------------------------------------------
void BTree::iterateChildrenSwips(void*, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   // Pre: bf is read locked
   auto& c_node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
   if (c_node.is_leaf) {
      return;
   }
   for (u16 i = 0; i < c_node.count; i++) {
      if (!callback(c_node.getChild(i).cast<BufferFrame>())) {
         return;
      }
   }
   callback(c_node.upper.cast<BufferFrame>());
}
// Helpers
// -------------------------------------------------------------------------------------
s64 BTree::iterateAllPagesRec(HybridPageGuard<BTreeNode>& node_guard, std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf)
{
   if (node_guard->is_leaf) {
      return leaf(node_guard.ref());
   }
   s64 res = inner(node_guard.ref());
   for (u16 i = 0; i < node_guard->count; i++) {
      Swip<BTreeNode>& c_swip = node_guard->getChild(i);
      auto c_guard = HybridPageGuard(node_guard, c_swip);
      c_guard.recheck_done();
      res += iterateAllPagesRec(c_guard, inner, leaf);
   }
   // -------------------------------------------------------------------------------------
   Swip<BTreeNode>& c_swip = node_guard->upper;
   auto c_guard = HybridPageGuard(node_guard, c_swip);
   c_guard.recheck_done();
   res += iterateAllPagesRec(c_guard, inner, leaf);
   // -------------------------------------------------------------------------------------
   return res;
}
// -------------------------------------------------------------------------------------
s64 BTree::iterateAllPages(std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf)
{
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
         HybridPageGuard c_guard(p_guard, p_guard->upper);
         jumpmu_return iterateAllPagesRec(c_guard, inner, leaf);
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
u32 BTree::countEntries()
{
   return iterateAllPages([](BTreeNode&) { return 0; }, [](BTreeNode& node) { return node.count; });
}
// -------------------------------------------------------------------------------------
u32 BTree::countPages()
{
   return iterateAllPages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 1; });
}
// -------------------------------------------------------------------------------------
u32 BTree::countInner()
{
   return iterateAllPages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 0; });
}
// -------------------------------------------------------------------------------------
double BTree::averageSpaceUsage()
{
   ensure(false);  // TODO
}
// -------------------------------------------------------------------------------------
u32 BTree::bytesFree()
{
   return iterateAllPages([](BTreeNode& inner) { return inner.freeSpaceAfterCompaction(); },
                          [](BTreeNode& leaf) { return leaf.freeSpaceAfterCompaction(); });
}
// -------------------------------------------------------------------------------------
void BTree::printInfos(uint64_t totalSize)
{
   HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
   HybridPageGuard r_guard(p_guard, p_guard->upper);
   uint64_t cnt = countPages();
   cout << "nodes:" << cnt << " innerNodes:" << countInner() << " space:" << (cnt * EFFECTIVE_PAGE_SIZE) / (float)totalSize << " height:" << height
        << " rootCnt:" << r_guard->count << " bytesFree:" << bytesFree() << endl;
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
