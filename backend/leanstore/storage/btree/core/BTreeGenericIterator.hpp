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
template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
class BTreePessimisticIterator : public BTreePessimisticIteratorInterface
{
   friend class BTreeGeneric;

  public:
   BTreeGeneric& btree;
   HybridPageGuard<BTreeNode> leaf;
   u16 fence_length;
   bool is_using_upper_fence;
   s32 cur = -1;
   u8 buffer[1024];
   // -------------------------------------------------------------------------------------
  public:
   BTreePessimisticIterator(BTreeGeneric& btree) : btree(btree) {}
   bool nextLeaf()
   {
      if (leaf->upper_fence.length == 0) {
         return false;
      } else {
         const u16 key_length = leaf->upper_fence.length + 1;
         u8 key[key_length];
         std::memcpy(key, leaf->getUpperFenceKey(), leaf->upper_fence.length);
         key[key_length - 1] = 0;
         leaf.unlock();
         btree.findLeafAndLatch<mode>(leaf, key, key_length);
         cur = leaf->lowerBound<false>(key, key_length);
         return true;
      }
   }
   bool prevLeaf()
   {
      if (leaf->lower_fence.length == 0) {
         return false;
      } else {
         const u16 key_length = leaf->lower_fence.length;
         u8 key[key_length];
         std::memcpy(key, leaf->getLowerFenceKey(), leaf->lower_fence.length);
         leaf.unlock();
         btree.findLeafAndLatch<mode>(leaf, key, key_length);
         cur = leaf->lowerBound<false>(key, key_length);
         if (cur == leaf->count) {
            cur -= 1;
         }
         return true;
      }
   }

  public:
   virtual OP_RESULT seekExact(Slice key) override
   {
      if (cur == -1 || leaf->compareKeyWithBoundaries(key.data(), key.length()) != 0) {
         btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
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
         btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
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
         btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
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
      while (true) {
         ensure(leaf.guard.state != GUARD_STATE::OPTIMISTIC);
         if ((cur + 1) < leaf->count) {
            cur += 1;
            return OP_RESULT::OK;
         } else if (leaf->upper_fence.length == 0) {
            return OP_RESULT::NOT_FOUND;
         } else {
            fence_length = leaf->upper_fence.length + 1;
            is_using_upper_fence = true;
            std::memcpy(buffer, leaf->getUpperFenceKey(), leaf->upper_fence.length);
            buffer[fence_length - 1] = 0;
            // -------------------------------------------------------------------------------------
            leaf.unlock();
            // ----------------------------------------------------------------------------------
            // Construct the next key (lower bound)
            btree.findLeafAndLatch<mode>(leaf, buffer, fence_length);
            // -------------------------------------------------------------------------------------
            if (leaf->count == 0) {
               continue;
            }
            cur = leaf->lowerBound<false>(buffer, fence_length);
            if (cur == leaf->count) {
               continue;
            }
            return OP_RESULT::OK;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT prev() override
   {
      while (true) {
         if ((cur - 1) >= 0) {
            cur -= 1;
            return OP_RESULT::OK;
         } else if (leaf->lower_fence.length == 0) {
            return OP_RESULT::NOT_FOUND;
         } else {
            fence_length = leaf->lower_fence.length;
            is_using_upper_fence = false;
            std::memcpy(buffer, leaf->getLowerFenceKey(), fence_length);
            // -------------------------------------------------------------------------------------
            leaf.unlock();
            // -------------------------------------------------------------------------------------
            btree.findLeafAndLatch<mode>(leaf, buffer, fence_length);
            // -------------------------------------------------------------------------------------
            if (leaf->count == 0) {
               continue;
            }
            bool is_equal = false;
            cur = leaf->lowerBound<false>(buffer, fence_length, &is_equal);
            if (is_equal) {
               return OP_RESULT::OK;
            } else if (cur > 0) {
               cur -= 1;
            } else {
               continue;
            }
         }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual Slice key() override
   {
      leaf->copyFullKey(cur, buffer);
      return Slice(buffer, leaf->getFullKeyLen(cur));
   }
   virtual bool isKeyEqualTo(Slice other) override { return other == key(); }
   virtual Slice keyPrefix() override { return Slice(leaf->getPrefix(), leaf->prefix_length); }
   virtual Slice keyWithoutPrefix() override { return Slice(leaf->getKey(cur), leaf->getKeyLen(cur)); }
   virtual u16 valueLength() { return leaf->getPayloadLength(cur); }
   virtual Slice value() override { return Slice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
};  // namespace btree
// -------------------------------------------------------------------------------------
using BTreeSharedIterator = BTreePessimisticIterator<LATCH_FALLBACK_MODE::SHARED>;
class BTreeExclusiveIterator : public BTreePessimisticIterator<LATCH_FALLBACK_MODE::EXCLUSIVE>
{
  public:
   BTreeExclusiveIterator(BTreeGeneric& btree) : BTreePessimisticIterator<LATCH_FALLBACK_MODE::EXCLUSIVE>(btree) {}
   virtual OP_RESULT seekToInsert(Slice key)
   {
      if (cur == -1 || leaf->compareKeyWithBoundaries(key.data(), key.length()) != 0) {
         btree.findLeafAndLatch<LATCH_FALLBACK_MODE::EXCLUSIVE>(leaf, key.data(), key.length());
      }
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal) {
         return OP_RESULT::DUPLICATE;
      } else {
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT canInsertInCurrentNode(Slice key, const u16 value_length)
   {
      return (leaf->canInsert(key.length(), value_length)) ? OP_RESULT::OK : OP_RESULT::NOT_ENOUGH_SPACE;
   }
   virtual void insertInCurrentNode(Slice key, u16 value_length)
   {
      assert(keyFitsInCurrentNode(key));
      assert(canInsertInCurrentNode(key, value_length) == OP_RESULT::OK);
      cur = leaf->insertDoNotCopyPayload(key.data(), key.length(), value_length);
   }
   virtual void insertInCurrentNode(Slice key, Slice value)
   {
      assert(keyFitsInCurrentNode(key));
      assert(canInsertInCurrentNode(key, value.length()) == OP_RESULT::OK);
      cur = leaf->insert(key.data(), key.length(), value.data(), value.length());
   }
   virtual bool keyFitsInCurrentNode(Slice key) { return leaf->compareKeyWithBoundaries(key.data(), key.length()) == 0; }
   virtual void splitForKey(Slice key)
   {
      while (true) {
         jumpmuTry()
         {
            if (cur == -1 || !keyFitsInCurrentNode(key)) {
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
      ret = canInsertInCurrentNode(key, value.length());
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
   virtual OP_RESULT replaceKV(Slice key, Slice value)
   {
   restart : {
      auto ret = seekExact(key);
      if (ret != OP_RESULT::OK) {
         return ret;
      }
      removeCurrent();
      if (canInsertInCurrentNode(key, value.length()) != OP_RESULT::OK) {
         splitForKey(key);
         goto restart;
      }
      insertInCurrentNode(key, value);
      return OP_RESULT::OK;
   }
   }
   // -------------------------------------------------------------------------------------
   virtual void shorten(const u16 new_size) { leaf->shortenPayload(cur, new_size); }
   // -------------------------------------------------------------------------------------
   virtual MutableSlice mutableValue() { return MutableSlice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
   // -------------------------------------------------------------------------------------
   virtual void contentionSplit()
   {
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
      if (!(cur >= 0 && cur < leaf->count)) {
         return OP_RESULT::OTHER;
      } else {
         leaf->removeSlot(cur);
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT removeKV(Slice key)
   {
      auto ret = BTreePessimisticIterator<LATCH_FALLBACK_MODE::EXCLUSIVE>::seekExact(key);
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
