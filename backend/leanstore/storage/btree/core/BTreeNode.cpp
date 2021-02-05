#include "BTreeNode.hpp"

#include "leanstore/sync-primitives/PageGuard.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
void BTreeNode::makeHint()
{
   u16 dist = count / (hint_count + 1);
   for (u16 i = 0; i < hint_count; i++)
      hint[i] = slot[dist * (i + 1)].head;
}
// -------------------------------------------------------------------------------------
void BTreeNode::updateHint(u16 slotId)
{
   u16 dist = count / (hint_count + 1);
   u16 begin = 0;
   if ((count > hint_count * 2 + 1) && (((count - 1) / (hint_count + 1)) == dist) && ((slotId / dist) > 1))
      begin = (slotId / dist) - 1;
   for (u16 i = begin; i < hint_count; i++)
      hint[i] = slot[dist * (i + 1)].head;
   for (u16 i = 0; i < hint_count; i++)
      assert(hint[i] == slot[dist * (i + 1)].head);
}
// -------------------------------------------------------------------------------------
u16 BTreeNode::spaceNeeded(u16 key_len, u16 payload_len, u16 prefix_len)
{
   return sizeof(Slot) + (key_len - prefix_len) + payload_len;
}
// -------------------------------------------------------------------------------------
u16 BTreeNode::spaceNeeded(u16 key_length, u16 payload_len)
{
   return spaceNeeded(key_length, payload_len, prefix_length);
}
// -------------------------------------------------------------------------------------
bool BTreeNode::canInsert(u16 key_len, u16 payload_len)
{
   const u16 space_needed = spaceNeeded(key_len, payload_len);
   if (!hasEnoughSpaceFor(space_needed))
      return false;  // no space, insert fails
   else
      return true;
}
// -------------------------------------------------------------------------------------
bool BTreeNode::prepareInsert(u16 key_len, u16 payload_len)
{
   const u16 space_needed = spaceNeeded(key_len, payload_len);
   if (!requestSpaceFor(space_needed))
      return false;  // no space, insert fails
   else
      return true;
}
// -------------------------------------------------------------------------------------
s16 BTreeNode::insertDoNotCopyPayload(const u8* key, u16 key_len, u16 payload_length)
{
   assert(canInsert(key_len, payload_length));
   prepareInsert(key_len, payload_length);
   // -------------------------------------------------------------------------------------
   s32 slotId = lowerBound<false>(key, key_len);
   memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
   // -------------------------------------------------------------------------------------
   // StoreKeyValue
   key += prefix_length;
   key_len -= prefix_length;
   slot[slotId].head = head(key, key_len);
   slot[slotId].key_len = key_len;
   slot[slotId].payload_len = payload_length;
   const u16 space = key_len + payload_length;
   data_offset -= space;
   space_used += space;
   slot[slotId].offset = data_offset;
   memcpy(getKey(slotId), key, key_len);
   // -------------------------------------------------------------------------------------
   count++;
   updateHint(slotId);
   return slotId;
}
// -------------------------------------------------------------------------------------
s32 BTreeNode::insert(const u8* key, u16 key_len, const u8* payload, u16 payload_length)
{
   DEBUG_BLOCK()
   {
      assert(canInsert(key_len, payload_length));
      s32 exact_pos = lowerBound<true>(key, key_len);
      static_cast<void>(exact_pos);
      assert(exact_pos == -1);  // assert for duplicates
   }
   // -------------------------------------------------------------------------------------
   prepareInsert(key_len, payload_length);
   s32 slotId = lowerBound<false>(key, key_len);
   memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
   storeKeyValue(slotId, key, key_len, payload, payload_length);
   count++;
   updateHint(slotId);
   return slotId;
   // -------------------------------------------------------------------------------------
   DEBUG_BLOCK()
   {
      s32 exact_pos = lowerBound<true>(key, key_len);
      static_cast<void>(exact_pos);
      assert(exact_pos == slotId);  // assert for duplicates
   }
}
// -------------------------------------------------------------------------------------
// TODO: probably broken
bool BTreeNode::update(u8*, u16, u16, u8*)
{
   ensure(false);
   return false;
}
// -------------------------------------------------------------------------------------
void BTreeNode::compactify()
{
   u16 should = freeSpaceAfterCompaction();
   static_cast<void>(should);
   BTreeNode tmp(is_leaf);
   tmp.setFences(getLowerFenceKey(), lower_fence.length, getUpperFenceKey(), upper_fence.length);
   copyKeyValueRange(&tmp, 0, 0, count);
   tmp.upper = upper;
   memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(BTreeNode));
   makeHint();
   assert(freeSpace() == should);  // TODO: why should ??
}
// -------------------------------------------------------------------------------------
u32 BTreeNode::mergeSpaceUpperBound(ExclusivePageGuard<BTreeNode>& right)
{
   assert(right->is_leaf);
   BTreeNode tmp(true);
   tmp.setFences(getLowerFenceKey(), lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
   u32 leftGrow = (prefix_length - tmp.prefix_length) * count;
   u32 rightGrow = (right->prefix_length - tmp.prefix_length) * right->count;
   u32 spaceUpperBound = space_used + right->space_used + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
   return spaceUpperBound;
}
// -------------------------------------------------------------------------------------
u32 BTreeNode::spaceUsedBySlot(u16 s_i)
{
   return sizeof(BTreeNode::Slot) + getKeyLen(s_i) + getPayloadLength(s_i);
}
// -------------------------------------------------------------------------------------
// right survives, this gets reclaimed
// left(this) into right
bool BTreeNode::merge(u16 slotId, ExclusivePageGuard<BTreeNode>& parent, ExclusivePageGuard<BTreeNode>& right)
{
   if (is_leaf) {
      assert(right->is_leaf);
      assert(parent->isInner());
      BTreeNode tmp(is_leaf);
      tmp.setFences(getLowerFenceKey(), lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
      u16 leftGrow = (prefix_length - tmp.prefix_length) * count;
      u16 rightGrow = (right->prefix_length - tmp.prefix_length) * right->count;
      u16 spaceUpperBound = space_used + right->space_used + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
      if (spaceUpperBound > EFFECTIVE_PAGE_SIZE) {
         return false;
      }
      copyKeyValueRange(&tmp, 0, 0, count);
      right->copyKeyValueRange(&tmp, count, 0, right->count);
      parent->removeSlot(slotId);
      memcpy(reinterpret_cast<u8*>(right.ptr()), &tmp, sizeof(BTreeNode));
      right->makeHint();
      return true;
   } else {  // Inner node
      assert(!right->is_leaf);
      assert(parent->isInner());
      BTreeNode tmp(is_leaf);
      tmp.setFences(getLowerFenceKey(), lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
      u16 leftGrow = (prefix_length - tmp.prefix_length) * count;
      u16 rightGrow = (right->prefix_length - tmp.prefix_length) * right->count;
      u16 extraKeyLength = parent->getFullKeyLen(slotId);
      u16 spaceUpperBound = space_used + right->space_used + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow +
                            spaceNeeded(extraKeyLength, sizeof(SwipType), tmp.prefix_length);
      if (spaceUpperBound > EFFECTIVE_PAGE_SIZE)
         return false;
      copyKeyValueRange(&tmp, 0, 0, count);
      u8 extraKey[extraKeyLength];
      parent->copyFullKey(slotId, extraKey);
      tmp.storeKeyValue(count, extraKey, extraKeyLength, reinterpret_cast<u8*>(&upper), sizeof(SwipType));
      tmp.count++;
      right->copyKeyValueRange(&tmp, tmp.count, 0, right->count);
      parent->removeSlot(slotId);
      tmp.upper = right->upper;
      tmp.makeHint();
      memcpy(reinterpret_cast<u8*>(right.ptr()), &tmp, sizeof(BTreeNode));
      return true;
   }
}
// -------------------------------------------------------------------------------------
void BTreeNode::storeKeyValue(u16 slotId, const u8* key, u16 key_len, const u8* payload, const u16 payload_len)
{
   // Head
   key += prefix_length;
   key_len -= prefix_length;
   // -------------------------------------------------------------------------------------
   slot[slotId].head = head(key, key_len);
   slot[slotId].key_len = key_len;
   slot[slotId].payload_len = payload_len;
   // Value
   const u16 space = key_len + payload_len;
   data_offset -= space;
   space_used += space;
   slot[slotId].offset = data_offset;
   // -------------------------------------------------------------------------------------
   memcpy(getKey(slotId), key, key_len);
   // -------------------------------------------------------------------------------------
   memcpy(getPayload(slotId), payload, payload_len);
   assert(ptr() + data_offset >= reinterpret_cast<u8*>(slot + count));
}
// -------------------------------------------------------------------------------------
// ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
void BTreeNode::copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, u16 count)
{
   if (prefix_length == dst->prefix_length) {
      // Fast path
      memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
      DEBUG_BLOCK()
      {
         u32 total_space_used = upper_fence.length + lower_fence.length;
         for (u16 i = 0; i < this->count; i++) {
            total_space_used += getKeyLen(i) + getPayloadLength(i);
         }
         assert(total_space_used == this->space_used);
      }
      for (u16 i = 0; i < count; i++) {
         u32 kv_size = getKeyLen(srcSlot + i) + getPayloadLength(srcSlot + i);
         dst->data_offset -= kv_size;
         dst->space_used += kv_size;
         dst->slot[dstSlot + i].offset = dst->data_offset;
         DEBUG_BLOCK()
         {
            [[maybe_unused]] s64 off_by = reinterpret_cast<u8*>(dst->slot + dstSlot + count) - (dst->ptr() + dst->data_offset);
            assert(off_by <= 0);
         }
         memcpy(dst->ptr() + dst->data_offset, ptr() + slot[srcSlot + i].offset, kv_size);
      }
   } else {
      for (u16 i = 0; i < count; i++)
         copyKeyValue(srcSlot + i, dst, dstSlot + i);
   }
   dst->count += count;
   assert((dst->ptr() + dst->data_offset) >= reinterpret_cast<u8*>(dst->slot + dst->count));
}
// -------------------------------------------------------------------------------------
void BTreeNode::copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot)
{
   u16 fullLength = getFullKeyLen(srcSlot);
   u8 key[fullLength];
   copyFullKey(srcSlot, key);
   dst->storeKeyValue(dstSlot, key, fullLength, getPayload(srcSlot), getPayloadLength(srcSlot));
}
// -------------------------------------------------------------------------------------
void BTreeNode::insertFence(BTreeNodeHeader::FenceKey& fk, u8* key, u16 keyLength)
{
   if (!key)
      return;
   assert(freeSpace() >= keyLength);
   data_offset -= keyLength;
   space_used += keyLength;
   fk.offset = data_offset;
   fk.length = keyLength;
   memcpy(ptr() + data_offset, key, keyLength);
}
// -------------------------------------------------------------------------------------
void BTreeNode::setFences(u8* lowerKey, u16 lowerLen, u8* upperKey, u16 upperLen)
{
   insertFence(lower_fence, lowerKey, lowerLen);
   insertFence(upper_fence, upperKey, upperLen);
   for (prefix_length = 0; (prefix_length < min(lowerLen, upperLen)) && (lowerKey[prefix_length] == upperKey[prefix_length]); prefix_length++)
      ;
}
// -------------------------------------------------------------------------------------
u16 BTreeNode::commonPrefix(u16 slotA, u16 slotB)
{
   if (count == 0) {  // Do not prefix compress if only one tuple is in to avoid corner cases (e.g., SI Version)
      return 0;
   } else {
      // TODO: the folowing two checks work only in single threaded
      //   assert(aPos < count);
      //   assert(bPos < count);
      u32 limit = min(slot[slotA].key_len, slot[slotB].key_len);
      u8 *a = getKey(slotA), *b = getKey(slotB);
      u32 i;
      for (i = 0; i < limit; i++)
         if (a[i] != b[i])
            break;
      return i;
   }
}
// -------------------------------------------------------------------------------------
BTreeNode::SeparatorInfo BTreeNode::findSep()
{
   if (isInner())
      return SeparatorInfo{getFullKeyLen(count / 2), static_cast<u16>(count / 2), false};

   u16 lower = count / 2 - count / 16;
   u16 upper = count / 2 + count / 16;
   // does not work under optimistic mode assert(upper < count);
   u16 maxPos = count / 2;
   s16 maxPrefix = commonPrefix(maxPos, 0);
   for (u32 i = lower; i < upper; i++) {
      s32 prefix = commonPrefix(i, 0);
      if (prefix > maxPrefix) {
         maxPrefix = prefix;
         maxPos = i;
      }
   }
   unsigned common = commonPrefix(maxPos, maxPos + 1);
   if ((slot[maxPos].key_len > common) && (slot[maxPos + 1].key_len > common + 1)) {
      return SeparatorInfo{static_cast<u16>(prefix_length + common + 1), maxPos, true};
   }
   return SeparatorInfo{getFullKeyLen(maxPos), maxPos, false};
}
// -------------------------------------------------------------------------------------
void BTreeNode::getSep(u8* sepKeyOut, BTreeNodeHeader::SeparatorInfo info)
{
   memcpy(sepKeyOut, getLowerFenceKey(), prefix_length);
   if (info.trunc) {
      memcpy(sepKeyOut + prefix_length, getKey(info.slot + 1), info.length - prefix_length);
   } else {
      memcpy(sepKeyOut + prefix_length, getKey(info.slot), info.length - prefix_length);
   }
}
// -------------------------------------------------------------------------------------
s32 BTreeNode::compareKeyWithBoundaries(const u8* key, u16 keyLength)
{
   // Lower Bound exclusive, upper bound inclusive
   if (lower_fence.offset) {
      int cmp = cmpKeys(key, getLowerFenceKey(), keyLength, lower_fence.length);
      if (!(cmp > 0))
         return 1;  // Key lower or equal LF
   }
   if (upper_fence.offset) {
      int cmp = cmpKeys(key, getUpperFenceKey(), keyLength, upper_fence.length);
      if (!(cmp <= 0))
         return -1;  // Key higher than UF
   }
   return 0;
}
// -------------------------------------------------------------------------------------
Swip<BTreeNode>& BTreeNode::lookupInner(const u8* key, u16 keyLength)
{
   s32 pos = lowerBound<false>(key, keyLength);
   if (pos == count)
      return upper;
   return getChild(pos);
}
// -------------------------------------------------------------------------------------
// This = right
void BTreeNode::split(ExclusivePageGuard<BTreeNode>& parent, ExclusivePageGuard<BTreeNode>& nodeLeft, u16 sepSlot, u8* sepKey, u16 sepLength)
{
   // PRE: current, parent and nodeLeft are x locked
   // assert(sepSlot > 0); TODO: really ?
   assert(sepSlot < (EFFECTIVE_PAGE_SIZE / sizeof(SwipType)));
   // -------------------------------------------------------------------------------------
   nodeLeft->setFences(getLowerFenceKey(), lower_fence.length, sepKey, sepLength);
   BTreeNode tmp(is_leaf);
   BTreeNode* nodeRight = &tmp;
   nodeRight->setFences(sepKey, sepLength, getUpperFenceKey(), upper_fence.length);
   assert(parent->canInsert(sepLength, sizeof(SwipType)));
   auto swip = nodeLeft.swip();
   parent->insert(sepKey, sepLength, reinterpret_cast<u8*>(&swip), sizeof(SwipType));
   if (is_leaf) {
      copyKeyValueRange(nodeLeft.ptr(), 0, 0, sepSlot + 1);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
   } else {
      copyKeyValueRange(nodeLeft.ptr(), 0, 0, sepSlot);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
      nodeLeft->upper = getChild(nodeLeft->count);
      nodeRight->upper = upper;
   }
   nodeLeft->makeHint();
   nodeRight->makeHint();
   // -------------------------------------------------------------------------------------
   memcpy(reinterpret_cast<char*>(this), nodeRight, sizeof(BTreeNode));
}
// -------------------------------------------------------------------------------------
bool BTreeNode::removeSlot(u16 slotId)
{
   space_used -= getKeyLen(slotId) + getPayloadLength(slotId);
   memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
   count--;
   makeHint();
   return true;
}
// -------------------------------------------------------------------------------------
bool BTreeNode::remove(const u8* key, const u16 keyLength)
{
   int slotId = lowerBound<true>(key, keyLength);
   if (slotId == -1)
      return false;  // key not found
   return removeSlot(slotId);
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
