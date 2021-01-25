#pragma once
#include "BTreeGeneric.hpp"
#include "BTreeIteratorinterface.hpp"
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
class BTreePessimisticIterator : public BTreeSharedIterator
{
   friend class BTreeGeneric;

  private:
   BTreeGeneric& btree;
   HybridPageGuard<BTreeNode> leaf;
   s32 cur = -1;
   u8 buffer[PAGE_SIZE];
   // -------------------------------------------------------------------------------------
   PessimisticIterator(BTreeGeneric& btree) : btree(btree) {}
   bool nextPage()
   {
      if (leaf->upper_fence.length == 0) {
         return false;
      } else {
         const u16 key_length = leaf->upper_fence.length + 1;
         u8 key[key_length];
         std::memcpy(key, leaf->getUpperFenceKey(), leaf->upper_fence.length);
         HybridPageGuard<BTreeNode> next_leaf;
         btree.findLeafAndLatch<mode>(next_leaf, key, key_length);
         leaf = std::move(next_leaf);
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
         HybridPageGuard<BTreeNode> next_leaf;
         btree.findLeafAndLatch<mode>(leaf, key, key_length);
         leaf = std::move(next_leaf);
         return true;
      }
   }

  public:
   virtual OP_RESULT seekExact(Slice key) override
   {
      btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal == true) {
         jumpmu_return OP_RESULT::OK;
      } else {
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT seek(Slice key) override
   {
      btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal == true) {
         jumpmu_return OP_RESULT::OK;
      } else if ((cur + 1) >= leaf->count) {
         if (nextPage() && leaf->count > 0) {
            cur = 0;
            jumpmu_return OP_RESULT::OK;
         } else {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      } else {
         cur += 1;
         jumpmu_return OP_RESULT::OK;
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT seekForPrev(Slice key) override
   {
      btree.findLeafAndLatch<mode>(leaf, key.data(), key.length());
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal == true) {
         jumpmu_return OP_RESULT::OK;
      } else if (cur == 0) {
         if (prevPage() && leaf->count > 0) {
            cur = leaf->count - 1;
            jumpmu_return OP_RESULT::OK;
         } else {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      } else {
         cur -= 1;
         jumpmu_return OP_RESULT::OK;
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
   virtual Slice key() override
   {
      leaf->copyFullKey(cur, buffer);
      return Slice(buffer, leaf->getFullKeyLen(cur));
   }
   virtual bool isKeyEqualTo(Slice key) override { return key == key(); }
   virtual Slice keyPrefix() override { return Slice(leaf->getPrefix(), leaf->prefix_length); }
   virtual Slice keyWithoutPrefix() override { return Slice(leaf->getKey(cur), leaf->getKeyLen(cur)); }
   virtual Slice value() override { return Slice(leaf->getPayload(cur), leaf->getPayloadLen(cur)); }
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
