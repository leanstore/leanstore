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
   s32 cur = -1;
   u8 buffer[PAGE_SIZE];
   bool prefix_copied = false;
   // -------------------------------------------------------------------------------------
  protected:
   void gotoPage(const Slice& key)
   {
      leaf.unlock();
      btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
      prefix_copied = false;
   }
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
         gotoPage(Slice(key, key_length));
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
         gotoPage(Slice(key, key_length));
         cur = leaf->lowerBound<false>(key, key_length);
         if (cur == leaf->count) {
            cur -= 1;
         }
         return true;
      }
   }

  public:
   OP_RESULT seekExactWithHint(Slice key, bool higher = true)  // EXP
   {
      ensure(cur != -1);
      cur = leaf->linearSearchWithHint(key.data(), key.length(), cur, higher);
      if (cur == -1) {
         return seekExact(key);
      } else {
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT seekExact(Slice key) override
   {
      if (cur == -1 || leaf->compareKeyWithBoundaries(key.data(), key.length()) != 0) {
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
      if (cur == leaf->count) {
         return OP_RESULT::NOT_FOUND;
      } else {
         return OP_RESULT::OK;
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
         if (prevLeaf() && cur < leaf->count) {
            return OP_RESULT::OK;
         } else {
            return OP_RESULT::NOT_FOUND;
         }
      } else {
         cur -= 1;
         return OP_RESULT::OK;
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT next() override
   {
      if ((cur + 1) < leaf->count) {
         cur += 1;
         return OP_RESULT::OK;
      } else {
      retry : {
         if (!nextLeaf()) {
            return OP_RESULT::NOT_FOUND;
         } else {
            if (leaf->count == 0) {
               goto retry;
            } else {
               return OP_RESULT::OK;
            }
         }
      }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT prev() override
   {
      if ((cur - 1) >= 0) {
         cur -= 1;
         return OP_RESULT::OK;
      } else {
      retry : {
         if (!prevLeaf()) {
            return OP_RESULT::NOT_FOUND;
         } else {
            if (leaf->count == 0) {
               goto retry;
            } else {
               return OP_RESULT::OK;
            }
         }
      }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual u8* keyBuffer() { return buffer; }
   // -------------------------------------------------------------------------------------
   virtual Slice key() override
   {
      if (!prefix_copied) {
         leaf->copyPrefix(buffer);
         prefix_copied = true;
      }
      leaf->copyKeyWithoutPrefix(cur, buffer + leaf->prefix_length);
      return Slice(buffer, leaf->getFullKeyLen(cur));
   }
   virtual bool isKeyEqualTo(Slice other) override { return other == key(); }
   virtual Slice keyPrefix() override { return Slice(leaf->getPrefix(), leaf->prefix_length); }
   virtual Slice keyWithoutPrefix() override { return Slice(leaf->getKey(cur), leaf->getKeyLen(cur)); }
   virtual u16 valueLength() { return leaf->getPayloadLength(cur); }
   virtual Slice value() override { return Slice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
};
// -------------------------------------------------------------------------------------
using BTreeSharedIterator = BTreePessimisticIterator<LATCH_FALLBACK_MODE::SHARED>;
class BTreeExclusiveIterator : public BTreePessimisticIterator<LATCH_FALLBACK_MODE::EXCLUSIVE>
{
  public:
   BTreeExclusiveIterator(BTreeGeneric& btree) : BTreePessimisticIterator<LATCH_FALLBACK_MODE::EXCLUSIVE>(btree) {}
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
      assert(enoughSpaceInCurrentNode(key, value_length) == OP_RESULT::OK);
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
   virtual bool keyInCurrentBoundaries(Slice key) { return leaf->compareKeyWithBoundaries(key.data(), key.length()) == 0; }
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
