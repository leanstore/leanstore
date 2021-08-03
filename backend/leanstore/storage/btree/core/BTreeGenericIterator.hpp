#pragma once
#include "BTreeGeneric.hpp"
#include "BTreeIteratorInterface.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
// Iterator
class BTreePessimisticIterator : public BTreePessimisticIteratorInterface
{
   friend class BTreeGeneric;

  public:
   BTreeGeneric& btree;
   const LATCH_FALLBACK_MODE mode;
   // -------------------------------------------------------------------------------------
   // Hooks
   std::function<void(HybridPageGuard<BTreeNode>& leaf)> before_changing_leaf_cb;
   std::function<void(HybridPageGuard<BTreeNode>& leaf)> after_changing_leaf_cb;
   // -------------------------------------------------------------------------------------
   s32 cur = -1;                        // Reset after every leaf change
   bool prefix_copied = false;          // Reset after every leaf change
   HybridPageGuard<BTreeNode> leaf;     // Reset after every leaf change
   HybridPageGuard<BTreeNode> p_guard;  // Reset after every leaf change
   s32 leaf_pos_in_parent = -1;         // Reset after every leaf change
   // -------------------------------------------------------------------------------------
   u8 buffer[PAGE_SIZE];
   // -------------------------------------------------------------------------------------
  protected:
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   void findLeafAndLatch(HybridPageGuard<BTreeNode>& target_guard, const u8* key, u16 key_length)
   {
      while (true) {
         leaf_pos_in_parent = -1;
         jumpmuTry()
         {
            target_guard.unlock();
            p_guard = HybridPageGuard<BTreeNode>(btree.meta_node_bf);
            target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
            // -------------------------------------------------------------------------------------
            u16 volatile level = 0;
            // -------------------------------------------------------------------------------------
            while (!target_guard->is_leaf) {
               WorkerCounters::myCounters().dt_inner_page[btree.dt_id]++;
               Swip<BTreeNode>* c_swip = nullptr;
               leaf_pos_in_parent = leaf->lowerBound<false>(key, key_length);
               if (leaf_pos_in_parent == leaf->count) {
                  c_swip = &leaf->upper;
               } else {
                  c_swip = &leaf->getChild(leaf_pos_in_parent);
               }
               p_guard = std::move(target_guard);
               if (level == btree.height - 1) {
                  target_guard = HybridPageGuard(p_guard, *c_swip, mode);
               } else {
                  target_guard = HybridPageGuard(p_guard, *c_swip);
               }
               level++;
            }
            // -------------------------------------------------------------------------------------
            p_guard.unlock();
            if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
               target_guard.toExclusive();
            } else {
               target_guard.toShared();
            }
            jumpmu_return;
         }
         jumpmuCatch() {}
      }
   }
   // -------------------------------------------------------------------------------------
   void gotoPage(const Slice& key)
   {
      COUNTERS_BLOCK()
      {
         if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
            WorkerCounters::myCounters().dt_goto_page_exec[btree.dt_id]++;
         } else {
            WorkerCounters::myCounters().dt_goto_page_shared[btree.dt_id]++;
         }
      }
      // -------------------------------------------------------------------------------------
      if (cur != -1 && before_changing_leaf_cb) {
         before_changing_leaf_cb(leaf);
      }
      // -------------------------------------------------------------------------------------
      leaf.unlock();
      if (mode == LATCH_FALLBACK_MODE::SHARED) {
         findLeafAndLatch<LATCH_FALLBACK_MODE::SHARED>(leaf, key.data(), key.length());
      } else if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
         findLeafAndLatch<LATCH_FALLBACK_MODE::EXCLUSIVE>(leaf, key.data(), key.length());
      } else {
         UNREACHABLE();
      }
      prefix_copied = false;
      // -------------------------------------------------------------------------------------
      if (after_changing_leaf_cb) {
         after_changing_leaf_cb(leaf);
      }
   }
   // -------------------------------------------------------------------------------------
   virtual bool keyInCurrentBoundaries(Slice key) { return leaf->compareKeyWithBoundaries(key.data(), key.length()) == 0; }
   // -------------------------------------------------------------------------------------
  public:
   BTreePessimisticIterator(BTreeGeneric& btree, const LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED) : btree(btree), mode(mode) {}
   // -------------------------------------------------------------------------------------
   void registerBeforeChangingLeafHook(std::function<void(HybridPageGuard<BTreeNode>& leaf)> cb) { before_changing_leaf_cb = cb; }
   void registerAfterChangingLeafHook(std::function<void(HybridPageGuard<BTreeNode>& leaf)> cb) { after_changing_leaf_cb = cb; }
   // -------------------------------------------------------------------------------------
   OP_RESULT seekExactWithHint(Slice key, bool higher = true)  // EXP
   {
      if (cur == -1) {
         return seekExact(key);
      }
      cur = leaf->linearSearchWithHint<true>(key.data(), key.length(), cur, higher);
      if (cur == -1) {
         return seekExact(key);
      } else {
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT seekExact(Slice key) override
   {
      if (cur == -1 || !keyInCurrentBoundaries(key)) {
         gotoPage(key);
      }
      cur = leaf->lowerBound<true>(key.data(), key.length());
      if (cur != -1) {
         return OP_RESULT::OK;
      } else {
         return OP_RESULT::NOT_FOUND;
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT seek(Slice key) override
   {
      if (cur == -1 || leaf->compareKeyWithBoundaries(key.data(), key.length()) != 0) {
         gotoPage(key);
      }
      cur = leaf->lowerBound<false>(key.data(), key.length());
      if (cur < leaf->count) {
         return OP_RESULT::OK;
      } else {
         return next();
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT seekForPrev(Slice key) override
   {
      if (cur == -1 || leaf->compareKeyWithBoundaries(key.data(), key.length()) != 0) {
         gotoPage(key);
      }
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal == true) {
         return OP_RESULT::OK;
      } else if (cur == 0) {
         return prev();
      } else {
         cur -= 1;
         return OP_RESULT::OK;
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT next() override
   {
      COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_next_tuple[btree.dt_id]++; }
      if ((cur + 1) < leaf->count) {
         cur += 1;
         return OP_RESULT::OK;
      } else {
         if (FLAGS_optimistic_scan) {
            jumpmuTry()
            {
               if ((leaf_pos_in_parent + 1) <= p_guard->count) {
                  // -------------------------------------------------------------------------------------
                  if (cur != -1 && before_changing_leaf_cb) {
                     before_changing_leaf_cb(leaf);
                  }
                  // -------------------------------------------------------------------------------------
                  s32 next_leaf_pos = leaf_pos_in_parent + 1;
                  Swip<BTreeNode>& c_swip = (next_leaf_pos < p_guard->count) ? p_guard->getChild(next_leaf_pos) : p_guard->upper;
                  leaf = HybridPageGuard(p_guard, c_swip, mode);
                  if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
                     leaf.toExclusive();
                  } else {
                     leaf.toShared();
                  }
                  leaf_pos_in_parent = next_leaf_pos;
                  cur = 0;
                  prefix_copied = false;
                  // -------------------------------------------------------------------------------------
                  if (after_changing_leaf_cb) {
                     after_changing_leaf_cb(leaf);
                  }
                  // -------------------------------------------------------------------------------------
                  jumpmu_return OP_RESULT::OK;
               }
            }
            jumpmuCatch() {}
         }
      retry : {
         if (leaf->upper_fence.length == 0) {
            return OP_RESULT::NOT_FOUND;
         } else {
            const u16 key_length = leaf->upper_fence.length + 1;
            u8 key[key_length];
            std::memcpy(key, leaf->getUpperFenceKey(), leaf->upper_fence.length);
            key[key_length - 1] = 0;
            gotoPage(Slice(key, key_length));
            // -------------------------------------------------------------------------------------
            if (leaf->count == 0) {
               COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_empty_leaf[btree.dt_id]++; }
               goto retry;
            }
            cur = leaf->lowerBound<false>(key, key_length);
            if (cur == leaf->count) {
               goto retry;
            }
            return OP_RESULT::OK;
         }
      }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT prev() override
   {
      COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_prev_tuple[btree.dt_id]++; }
      if ((cur - 1) >= 0) {
         cur -= 1;
         return OP_RESULT::OK;
      } else {
         if (FLAGS_optimistic_scan) {
            jumpmuTry()
            {
               if (leaf_pos_in_parent > 0) {
                  // -------------------------------------------------------------------------------------
                  if (cur != -1 && before_changing_leaf_cb) {
                     before_changing_leaf_cb(leaf);
                  }
                  // -------------------------------------------------------------------------------------
                  s32 prev_leaf_pos = leaf_pos_in_parent - 1;
                  Swip<BTreeNode>& c_swip = p_guard->getChild(prev_leaf_pos);
                  leaf = HybridPageGuard(p_guard, c_swip, mode);
                  if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
                     leaf.toExclusive();
                  } else {
                     leaf.toShared();
                  }
                  leaf_pos_in_parent = prev_leaf_pos;
                  cur = 0;
                  prefix_copied = false;
                  // -------------------------------------------------------------------------------------
                  if (after_changing_leaf_cb) {
                     after_changing_leaf_cb(leaf);
                  }
                  // -------------------------------------------------------------------------------------
                  jumpmu_return OP_RESULT::OK;
               }
            }
            jumpmuCatch() {}
         }
      retry : {
         if (leaf->lower_fence.length == 0) {
            return OP_RESULT::OK;
         } else {
            const u16 key_length = leaf->lower_fence.length;
            u8 key[key_length];
            std::memcpy(key, leaf->getLowerFenceKey(), leaf->lower_fence.length);
            gotoPage(Slice(key, key_length));
            // -------------------------------------------------------------------------------------
            if (leaf->count == 0) {
               goto retry;
            } else {
               bool is_equal = false;
               cur = leaf->lowerBound<false>(key, key_length, &is_equal);
               ensure(is_equal || cur == leaf->count);
               if (is_equal) {
                  return OP_RESULT::OK;
               }
               // Must go one step backward
               if (cur > 0) {
                  cur -= 1;
                  return OP_RESULT::OK;
               } else {
                  goto retry;
               }
            }
         }
      }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual void assembleKey()
   {
      if (!prefix_copied) {
         leaf->copyPrefix(buffer);
         prefix_copied = true;
      }
      leaf->copyKeyWithoutPrefix(cur, buffer + leaf->prefix_length);
   }
   virtual Slice key() override { return Slice(buffer, leaf->getFullKeyLen(cur)); }
   virtual MutableSlice mutableKeyInBuffer() { return MutableSlice(buffer, leaf->getFullKeyLen(cur)); }
   virtual MutableSlice mutableKeyInBuffer(u16 size)
   {
      assert(size < PAGE_SIZE);
      return MutableSlice(buffer, size);
   }
   // -------------------------------------------------------------------------------------
   virtual bool isKeyEqualTo(Slice other) override
   {
      ensure(false);
      return other == key();
   }
   virtual Slice keyPrefix() override { return Slice(leaf->getPrefix(), leaf->prefix_length); }
   virtual Slice keyWithoutPrefix() override { return Slice(leaf->getKey(cur), leaf->getKeyLen(cur)); }
   virtual u16 valueLength() { return leaf->getPayloadLength(cur); }
   virtual Slice value() override { return Slice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
};  // namespace btree
// -------------------------------------------------------------------------------------
class BTreeSharedIterator : public BTreePessimisticIterator
{
  public:
   BTreeSharedIterator(BTreeGeneric& btree, const LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED) : BTreePessimisticIterator(btree, mode) {}
};
// -------------------------------------------------------------------------------------
class BTreeExclusiveIterator : public BTreePessimisticIterator
{
  private:
  public:
   BTreeExclusiveIterator(BTreeGeneric& btree) : BTreePessimisticIterator(btree, LATCH_FALLBACK_MODE::EXCLUSIVE) {}
   BTreeExclusiveIterator(BTreeGeneric& btree, BufferFrame* bf, const u64 bf_version)
       : BTreePessimisticIterator(btree, LATCH_FALLBACK_MODE::EXCLUSIVE)
   {
      Guard as_it_was_witnessed(bf->header.latch, bf_version);
      leaf = HybridPageGuard<BTreeNode>(std::move(as_it_was_witnessed), bf);
      leaf.toExclusive();
   }
   // -------------------------------------------------------------------------------------
   void markAsDirty() { leaf.incrementGSN(); }
   virtual OP_RESULT seekToInsertWithHint(Slice key, bool higher = true)
   {
      ensure(cur != -1);
      cur = leaf->linearSearchWithHint(key.data(), key.length(), cur, higher);
      if (cur == -1) {
         return seekToInsert(key);
      } else {
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT seekToInsert(Slice key)
   {
      if (cur == -1 || !keyInCurrentBoundaries(key)) {
         gotoPage(key);
      }
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal) {
         return OP_RESULT::DUPLICATE;
      } else {
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT enoughSpaceInCurrentNode(Slice key, const u16 value_length)
   {
      return (leaf->canInsert(key.length(), value_length)) ? OP_RESULT::OK : OP_RESULT::NOT_ENOUGH_SPACE;
   }
   virtual void insertInCurrentNode(Slice key, u16 value_length)
   {
      assert(keyInCurrentBoundaries(key));
      ensure(enoughSpaceInCurrentNode(key, value_length) == OP_RESULT::OK);
      cur = leaf->insertDoNotCopyPayload(key.data(), key.length(), value_length, cur);
   }
   virtual void insertInCurrentNode(Slice key, Slice value)
   {
      assert(keyInCurrentBoundaries(key));
      assert(enoughSpaceInCurrentNode(key, value.length()) == OP_RESULT::OK);
      assert(cur != -1);
      cur = leaf->insertDoNotCopyPayload(key.data(), key.length(), value.length(), cur);
      std::memcpy(leaf->getPayload(cur), value.data(), value.length());
   }
   virtual void splitForKey(Slice key)
   {
      while (true) {
         jumpmuTry()
         {
            if (cur == -1 || !keyInCurrentBoundaries(key)) {
               btree.findLeafCanJump<LATCH_FALLBACK_MODE::SHARED>(leaf, key.data(), key.length());
            }
            BufferFrame* bf = leaf.bf;
            leaf.unlock();
            cur = -1;
            // -------------------------------------------------------------------------------------
            btree.trySplit(*bf);
            jumpmu_break;
         }
         jumpmuCatch() {}
      }
   }
   virtual OP_RESULT insertKV(Slice key, Slice value)
   {
      OP_RESULT ret;
   restart : {
      ret = seekToInsert(key);
      if (ret != OP_RESULT::OK) {
         return ret;
      }
      ret = enoughSpaceInCurrentNode(key, value.length());
      if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
         splitForKey(key);
         goto restart;
      } else if (ret == OP_RESULT::OK) {
         insertInCurrentNode(key, value);
         return OP_RESULT::OK;
      } else {
         return ret;
      }
   }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT replaceKV(Slice, Slice)
   {
      ensure(false);
      return OP_RESULT::NOT_FOUND;
   }
   // -------------------------------------------------------------------------------------
   // The caller must retain the payload when using any of the following payload resize functions
   virtual void shorten(const u16 new_size) { leaf->shortenPayload(cur, new_size); }
   // -------------------------------------------------------------------------------------
   void extendPayload(const u16 new_length)
   {
      ensure(cur != -1 && new_length > leaf->getPayloadLength(cur));
      OP_RESULT ret;
      while (!leaf->canExtendPayload(cur, new_length)) {
         assembleKey();
         Slice key = this->key();
         splitForKey(key);
         ret = seekExact(key);
         ensure(ret == OP_RESULT::OK);
      }
      assert(cur != -1);
      leaf->extendPayload(cur, new_length);
   }
   // -------------------------------------------------------------------------------------
   virtual MutableSlice mutableValue() { return MutableSlice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
   // -------------------------------------------------------------------------------------
   virtual void contentionSplit()
   {
      if (!FLAGS_contention_split) {
         return;
      }
      const u64 random_number = utils::RandomGenerator::getRandU64();
      if ((random_number & ((1ull << FLAGS_cm_update_on) - 1)) == 0) {
         s64 last_modified_pos = leaf.bf->header.contention_tracker.last_modified_pos;
         leaf.bf->header.contention_tracker.last_modified_pos = cur;
         leaf.bf->header.contention_tracker.restarts_counter += leaf.hasFacedContention();
         leaf.bf->header.contention_tracker.access_counter++;
         if ((random_number & ((1ull << FLAGS_cm_period) - 1)) == 0) {
            const u64 current_restarts_counter = leaf.bf->header.contention_tracker.restarts_counter;
            const u64 current_access_counter = leaf.bf->header.contention_tracker.access_counter;
            const u64 normalized_restarts = 100.0 * current_restarts_counter / current_access_counter;
            leaf.bf->header.contention_tracker.restarts_counter = 0;
            leaf.bf->header.contention_tracker.access_counter = 0;
            // -------------------------------------------------------------------------------------
            if (last_modified_pos != cur && normalized_restarts >= FLAGS_cm_slowpath_threshold && leaf->count > 2) {
               s16 split_pos = std::min<s16>(last_modified_pos, cur);
               leaf.unlock();
               cur = -1;
               jumpmuTry()
               {
                  btree.trySplit(*leaf.bf, split_pos);
                  WorkerCounters::myCounters().contention_split_succ_counter[btree.dt_id]++;
               }
               jumpmuCatch() { WorkerCounters::myCounters().contention_split_fail_counter[btree.dt_id]++; }
            }
         }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT removeCurrent()
   {
      if (!(leaf.bf != nullptr && cur >= 0 && cur < leaf->count)) {
         ensure(false);
         return OP_RESULT::OTHER;
      } else {
         leaf->removeSlot(cur);
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT removeKV(Slice key)
   {
      auto ret = seekExact(key);
      if (ret == OP_RESULT::OK) {
         leaf->removeSlot(cur);
         return OP_RESULT::OK;
      } else {
         return ret;
      }
   }
   virtual void mergeIfNeeded()
   {
      if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
         leaf.unlock();
         cur = -1;
         jumpmuTry() { btree.tryMerge(*leaf.bf); }
         jumpmuCatch()
         {
            // nothing, it is fine not to merge
         }
      }
   }
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
