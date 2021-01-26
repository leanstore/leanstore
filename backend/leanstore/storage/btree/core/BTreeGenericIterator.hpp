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
   u8 buffer[1024];
   // -------------------------------------------------------------------------------------
  public:
   BTreePessimisticIterator(BTreeGeneric& btree) : btree(btree) {}
   // TODO: What if the current node got merged after we release the latch
   bool nextPage()
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
         return true;
      }
   }
   bool prevPage()
   {
      if (leaf->lower_fence.length == 0) {
         return false;
      } else {
         const u16 key_length = leaf->lower_fence.length;
         u8 key[key_length];
         std::memcpy(key, leaf->getLowerFenceKey(), leaf->lower_fence.length);
         leaf.unlock();
         btree.findLeafAndLatch<mode>(leaf, key, key_length);
         return true;
      }
   }

  public:
   virtual OP_RESULT seekExact(Slice key) override
   {
      btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
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
      btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal == true) {
         return OP_RESULT::OK;
      } else if ((cur + 1) >= leaf->count) {
         if (nextPage() && leaf->count > 0) {
            cur = 0;
            return OP_RESULT::OK;
         } else {
            return OP_RESULT::NOT_FOUND;
         }
      } else {
         cur += 1;
         return OP_RESULT::OK;
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT seekForPrev(Slice key) override
   {
      btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal == true) {
         return OP_RESULT::OK;
      } else if (cur == 0) {
         if (prevPage() && leaf->count > 0) {
            cur = leaf->count - 1;
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
         if (nextPage() && leaf->count > 0) {
            cur = 0;
            return OP_RESULT::OK;
         } else {
            return OP_RESULT::NOT_FOUND;
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
         if (prevPage() && leaf->count > 0) {
            cur = leaf->count - 1;
            return OP_RESULT::OK;
         } else {
            return OP_RESULT::NOT_FOUND;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual Slice key()override
   {
      leaf->copyFullKey(cur, buffer);
      return Slice(buffer, leaf->getFullKeyLen(cur));
   }
   virtual bool isKeyEqualTo(Slice other) override { return other == key(); }
   virtual Slice keyPrefix() override { return Slice(leaf->getPrefix(), leaf->prefix_length); }
   virtual Slice keyWithoutPrefix() override{ return Slice(leaf->getKey(cur), leaf->getKeyLen(cur)); }
   virtual u16 valueLength() { return leaf->getPayloadLength(cur); }
   virtual Slice value() override { return Slice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
};
// -------------------------------------------------------------------------------------
using BTreeSharedIterator = BTreePessimisticIterator<LATCH_FALLBACK_MODE::SHARED>;
class BTreeExclusiveIterator : public BTreePessimisticIterator<LATCH_FALLBACK_MODE::EXCLUSIVE>
{
  public:
   BTreeExclusiveIterator(BTreeGeneric& btree) : BTreePessimisticIterator<LATCH_FALLBACK_MODE::EXCLUSIVE>(btree) {}
   virtual void seekToInsert(Slice key)
   {
      btree.findLeafAndLatch<LATCH_FALLBACK_MODE::EXCLUSIVE>(leaf, key.data(), key.length());
      cur = 0;
   }
   virtual OP_RESULT canInsertInCurrentNode(Slice key, Slice value)
   {
      if (leaf->prepareInsert(key.data(), key.length(), value.length())) {
         return OP_RESULT::OK;
      } else {
         return OP_RESULT::NOT_ENOUGH_SPACE;
      }
   }
   virtual void insertInCurrentNode(Slice key, Slice value)
   {
      DEBUG_BLOCK() { assert(canInsertInCurrentNode(key, value) == OP_RESULT::OK); }
      leaf->insert(key.data(), key.length(), value.data(), value.length());
   }
   virtual OP_RESULT splitForKey(Slice key)
   {
      while (true) {
         jumpmuTry()
         {
            btree.findLeafCanJump<LATCH_FALLBACK_MODE::SHARED>(leaf, key.data(), key.length());
            auto bf = leaf.bf;
            leaf.unlock();
            // -------------------------------------------------------------------------------------
            btree.trySplit(*bf);
            seekToInsert(key);
            jumpmu_return OP_RESULT::OK;
         }
         jumpmuCatch() {}
      }
   }
   virtual OP_RESULT insertKV(Slice key, Slice value)
   {
   restart : {
      seekToInsert(key);
      auto ret = canInsertInCurrentNode(key, value);
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
      auto ret = seekExact(key);
      if (ret != OP_RESULT::OK) {
         return ret;
      }
      removeCurrent();
      if (canInsertInCurrentNode(key, value) != OP_RESULT::OK) {
         splitForKey(key);
      }
      insertInCurrentNode(key, value);
      return OP_RESULT::OK;
   }
   virtual u8* valuePtr() { return leaf->getPayload(cur); }
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
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
