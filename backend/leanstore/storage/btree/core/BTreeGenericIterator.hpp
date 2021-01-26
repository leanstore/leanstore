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
class BTreePessimisticIterator : public BTreeSharedIterator
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
         leaf.kill();
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
         leaf.kill();
         btree.findLeafAndLatch<mode>(leaf, key, key_length);
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
   virtual Slice key() override
   {
      leaf->copyFullKey(cur, buffer);
      return Slice(buffer, leaf->getFullKeyLen(cur));
   }
   virtual bool isKeyEqualTo(Slice other) override { return other == key(); }
   virtual Slice keyPrefix() override { return Slice(leaf->getPrefix(), leaf->prefix_length); }
   virtual Slice keyWithoutPrefix() override { return Slice(leaf->getKey(cur), leaf->getKeyLen(cur)); }
   virtual Slice value() override { return Slice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
};  // namespace btree
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
