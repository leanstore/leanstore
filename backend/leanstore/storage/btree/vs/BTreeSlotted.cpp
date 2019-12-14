#include "BTreeSlotted.hpp"
#include "leanstore/storage/buffer-manager/PageGuard.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace btree {
namespace vs {
// -------------------------------------------------------------------------------------
unsigned BTreeNode::spaceNeeded(unsigned keyLength, unsigned prefixLength)
{
   assert(keyLength >= prefixLength);
   unsigned restLen = keyLength - prefixLength;
   if ( restLen <= 4 )
      return sizeof(Slot) + sizeof(ValueType);
   restLen -= sizeof(SketchType);
   return sizeof(Slot) + restLen + sizeof(ValueType) + ((restLen > largeLimit) ? sizeof(u16) : 0);
}
// -------------------------------------------------------------------------------------
int BTreeNode::cmpKeys(u8 *a, u8 *b, unsigned aLength, unsigned bLength)
{
   int c = memcmp(a, b, min(aLength, bLength));
   if ( c )
      return c;
   return (aLength - bLength);

}
// -------------------------------------------------------------------------------------
SketchType BTreeNode::head(u8 *&key, unsigned &keyLength)
{
   SketchType result;
   if ( keyLength > 3 ) {
      result = swap(*reinterpret_cast<u32 *>(key));
      key += sizeof(SketchType);
      keyLength -= sizeof(SketchType);
      return result;
   }
   switch ( keyLength ) {
      case 0:
         result = 0;
         break;
      case 1:
         result = static_cast<u32>(key[0]) << 24;
         break;
      case 2:
         result = static_cast<u32>(swap(*reinterpret_cast<u16 *>(key))) << 16;
         break;
      case 3:
         result = (static_cast<u32>(swap(*reinterpret_cast<u16 *>(key))) << 16) | (static_cast<u32>(key[2]) << 8);
         break;
      default:
         __builtin_unreachable();
   }
   key += keyLength; // should not be needed
   keyLength = 0;
   return result;
}
// -------------------------------------------------------------------------------------
void BTreeNode::makeHint()
{
   unsigned dist = count / (hint_count + 1);
   for ( unsigned i = 0; i < hint_count; i++ )
      hint[i] = slot[dist * (i + 1)].sketch;
}
// -------------------------------------------------------------------------------------
void BTreeNode::updateHint(unsigned slotId)
{
   unsigned dist = count / (hint_count + 1);
   unsigned begin = 0;
   if ((count > hint_count * 2 + 1) && (((count - 1) / (hint_count + 1)) == dist) && ((slotId / dist) > 1))
      begin = (slotId / dist) - 1;
   for ( unsigned i = begin; i < hint_count; i++ )
      hint[i] = slot[dist * (i + 1)].sketch;
   for ( unsigned i = 0; i < hint_count; i++ )
      assert(hint[i] == slot[dist * (i + 1)].sketch);
}
// -------------------------------------------------------------------------------------
bool BTreeNode::insert(u8 *key, unsigned keyLength, ValueType value, u8 *payload)
{
   assert(sanityCheck(key, keyLength));
   // -------------------------------------------------------------------------------------
   const u16 space_needed = (is_leaf) ? value.raw() + spaceNeeded(keyLength, prefix_length) : spaceNeeded(keyLength, prefix_length);
   if ( !requestSpaceFor(space_needed))
      return false; // no space, insert fails

   s32 slotId = lowerBound<false>(key, keyLength);
   memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
   storeKeyValue(slotId, key, keyLength, value, payload);
   count++;
   updateHint(slotId);
   assert(lowerBound<true>(key, keyLength) == slotId); // assert for duplicates
   return true;
}
// -------------------------------------------------------------------------------------
bool BTreeNode::update(u8 *key, unsigned keyLength, u16 payload_length, u8 *payload)
{
   s32 slotId = lowerBound<true>(key, keyLength);
   if ( slotId == -1 ) {
      // this happens after we remove the slot and not be able to insert directly without a split
      return insert(key, keyLength, ValueType(reinterpret_cast<BufferFrame *>(payload_length)), payload);
   }
   s32 space_needed = payload_length - getPayloadLength(slotId);
   if ( space_needed == 0 ) {
      memcpy(isLarge(slotId) ? getPayloadLarge(slotId) : getPayload(slotId), payload, payload_length);
      return true;
   } else {
      removeSlot(slotId);
      return insert(key, keyLength, ValueType(reinterpret_cast<BufferFrame *>(payload_length)), payload);
   }
}
// -------------------------------------------------------------------------------------
void BTreeNode::compactify()
{
   unsigned should = freeSpaceAfterCompaction();
   static_cast<void>(should);
   BTreeNode tmp(is_leaf);
   tmp.setFences(getLowerFenceKey(), lower_fence.length, getUpperFenceKey(), upper_fence.length);
   copyKeyValueRange(&tmp, 0, 0, count);
   tmp.upper = upper;
   memcpy(reinterpret_cast<char *>(this), &tmp, sizeof(BTreeNode));
   makeHint();
   assert(freeSpace() == should);
}
// -------------------------------------------------------------------------------------
bool BTreeNode::merge(unsigned slotId, WritePageGuard<BTreeNode> &parent, WritePageGuard<BTreeNode> &right)
{
   if ( is_leaf ) {
      assert(right->is_leaf);
      assert(parent->isInner());
      BTreeNode tmp(is_leaf);
      tmp.setFences(getLowerFenceKey(), lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
      unsigned leftGrow = (prefix_length - tmp.prefix_length) * count;
      unsigned rightGrow = (right->prefix_length - tmp.prefix_length) * right->count;
      unsigned spaceUpperBound = space_used + right->space_used + (reinterpret_cast<u8 *>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
      if ( spaceUpperBound > EFFECTIVE_PAGE_SIZE )
         return false;
      copyKeyValueRange(&tmp, 0, 0, count);
      right->copyKeyValueRange(&tmp, count, 0, right->count);
      parent->removeSlot(slotId);
      memcpy(reinterpret_cast<u8 *>(right.ptr()), &tmp, sizeof(BTreeNode));
      right->makeHint();
      return true;
   } else {
      assert(!right->is_leaf);
      assert(parent->isInner());
      BTreeNode tmp(is_leaf);
      tmp.setFences(getLowerFenceKey(), lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
      unsigned leftGrow = (prefix_length - tmp.prefix_length) * count;
      unsigned rightGrow = (right->prefix_length - tmp.prefix_length) * right->count;
      unsigned extraKeyLength = parent->getFullKeyLength(slotId);
      unsigned spaceUpperBound = space_used + right->space_used + (reinterpret_cast<u8 *>(slot + count + right->count) - ptr()) + leftGrow + rightGrow + spaceNeeded(extraKeyLength, tmp.prefix_length);
      if ( spaceUpperBound > EFFECTIVE_PAGE_SIZE )
         return false;
      copyKeyValueRange(&tmp, 0, 0, count);
      u8 extraKey[extraKeyLength];
      parent->copyFullKey(slotId, extraKey, extraKeyLength);
      tmp.storeKeyValue(count, extraKey, extraKeyLength, upper);
      tmp.count++;
      right->copyKeyValueRange(&tmp, tmp.count, 0, right->count);
      parent->removeSlot(slotId);
      tmp.upper = right->upper;
      tmp.makeHint();
      memcpy(reinterpret_cast<u8 *>(right.ptr()), &tmp, sizeof(BTreeNode));
      return true;
   }
}
// -------------------------------------------------------------------------------------
void BTreeNode::storeKeyValue(u16 slotId, u8 *key, unsigned keyLength, ValueType value, u8 *payload)
{

   // Head
   key += prefix_length;
   keyLength -= prefix_length;
   slot[slotId].head_len = (keyLength >= sizeof(SketchType)) ? sizeof(SketchType) : keyLength;
   slot[slotId].sketch = head(key, keyLength);
   // Value
   unsigned space = keyLength + sizeof(ValueType) + ((keyLength > largeLimit) ? sizeof(u16) : 0) + ((is_leaf) ? value.raw() : 0);
   data_offset -= space;
   space_used += space;
   slot[slotId].offset = data_offset;
   getValue(slotId) = value;
   // Rest
   if ( keyLength > largeLimit ) { // large string
      setLarge(slotId);
      getRestLenLarge(slotId) = keyLength;
      memcpy(getRestLarge(slotId), key, keyLength);
      if ( is_leaf ) { // AAA
         assert(payload != nullptr);
         memcpy(getPayloadLarge(slotId), payload, getPayloadLength(slotId));
      }
   } else { // normal string
      slot[slotId].rest_len = keyLength;
      memcpy(getRest(slotId), key, keyLength);
      if ( is_leaf ) { // AAA
         assert(payload != nullptr);
         memcpy(getPayload(slotId), payload, getPayloadLength(slotId));
      }
   }
   // store payload
}
// -------------------------------------------------------------------------------------
void BTreeNode::copyKeyValueRange(BTreeNode *dst, u16 dstSlot, u16 srcSlot, unsigned count)
{
   if ( prefix_length == dst->prefix_length ) {
      // Fast path
      memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
      for ( unsigned i = 0; i < count; i++ ) {
         unsigned space = sizeof(ValueType) + (isLarge(srcSlot + i) ? (getRestLenLarge(srcSlot + i) + sizeof(u16)) : getRestLen(srcSlot + i));
         if ( dst->is_leaf ) {
            assert(is_leaf);
            space += getPayloadLength(srcSlot + i); // AAA: Payload size
         }
         dst->data_offset -= space;
         dst->space_used += space;
         dst->slot[dstSlot + i].offset = dst->data_offset;
         memcpy(reinterpret_cast<u8 *>(dst) + dst->data_offset, ptr() + slot[srcSlot + i].offset, space);
      }
   } else {
      for ( unsigned i = 0; i < count; i++ )
         copyKeyValue(srcSlot + i, dst, dstSlot + i);
   }
   dst->count += count;
   assert((ptr() + dst->data_offset) >= reinterpret_cast<u8 *>(slot + count));
}
// -------------------------------------------------------------------------------------
void BTreeNode::copyKeyValue(u16 srcSlot, BTreeNode *dst, u16 dstSlot)
{
   unsigned fullLength = getFullKeyLength(srcSlot);
   u8 key[fullLength];
   copyFullKey(srcSlot, key, fullLength);
   u8 *payload = (is_leaf) ? ((isLarge(srcSlot) ? getPayloadLarge(srcSlot) : getPayload(srcSlot))) : nullptr;
   dst->storeKeyValue(dstSlot, key, fullLength, getValue(srcSlot), payload);
}
// -------------------------------------------------------------------------------------
void BTreeNode::insertFence(BTreeNodeHeader::FenceKey &fk, u8 *key, unsigned keyLength)
{
   if ( !key )
      return;
   assert(freeSpace() >= keyLength);
   data_offset -= keyLength;
   space_used += keyLength;
   fk.offset = data_offset;
   fk.length = keyLength;
   memcpy(ptr() + data_offset, key, keyLength);
}
// -------------------------------------------------------------------------------------
void BTreeNode::setFences(u8 *lowerKey, unsigned lowerLen, u8 *upperKey, unsigned upperLen)
{
   insertFence(lower_fence, lowerKey, lowerLen);
   insertFence(upper_fence, upperKey, upperLen);
   for ( prefix_length = 0; (prefix_length < min(lowerLen, upperLen)) && (lowerKey[prefix_length] == upperKey[prefix_length]); prefix_length++ );
}
// -------------------------------------------------------------------------------------
unsigned BTreeNode::commonPrefix(unsigned aPos, unsigned bPos)
{
   // TODO: the folowing two checks work only in single threaded
//   assert(aPos < count);
//   assert(bPos < count);
   if ((slot[aPos].sketch == slot[bPos].sketch) && (slot[aPos].head_len == slot[bPos].head_len)) {
      unsigned aLen, bLen;
      u8 *a, *b;
      if ( isLarge(aPos)) {
         a = getRestLarge(aPos);
         aLen = getRestLenLarge(aPos);
      } else {
         a = getRest(aPos);
         aLen = getRestLen(aPos);
      }
      if ( isLarge(bPos)) {
         b = getRestLarge(bPos);
         bLen = getRestLenLarge(bPos);
      } else {
         b = getRest(bPos);
         bLen = getRestLen(bPos);
      }
      unsigned i;
      for ( i = 0; i < min(aLen, bLen); i++ )
         if ( a[i] != b[i] )
            break;
      return i + slot[aPos].head_len;
   }
   unsigned limit = min(slot[aPos].head_len, slot[bPos].head_len);
   unsigned i;
   for ( i = 0; i < limit; i++ )
      if ( slot[aPos].sketch_bytes[3 - i] != slot[bPos].sketch_bytes[3 - i] )
         return i;
   return i;
}
// -------------------------------------------------------------------------------------
BTreeNode::SeparatorInfo BTreeNode::findSep()
{
   if ( isInner())
      return SeparatorInfo{getFullKeyLength(count / 2), static_cast<unsigned>(count / 2), false};

   unsigned lower = count / 2 - count / 16;
   unsigned upper = count / 2 + count / 16;
//   assert(upper < count); TODO
   unsigned maxPos = count / 2;
   int maxPrefix = commonPrefix(maxPos, 0);
   for ( unsigned i = lower; i < upper; i++ ) {
      int prefix = commonPrefix(i, 0);
      if ( prefix > maxPrefix ) {
         maxPrefix = prefix;
         maxPos = i;
      }
   }
   unsigned common = commonPrefix(maxPos, maxPos + 1);
   if ((common > sizeof(SketchType)) && (getFullKeyLength(maxPos) - prefix_length > common) && (getFullKeyLength(maxPos + 1) - prefix_length > common + 2)) {
      return SeparatorInfo{static_cast<unsigned>(prefix_length + common + 1), maxPos, true};
   }
   return SeparatorInfo{getFullKeyLength(maxPos), maxPos, false};
}
// -------------------------------------------------------------------------------------
void BTreeNode::getSep(u8 *sepKeyOut, BTreeNodeHeader::SeparatorInfo info)
{
   copyFullKey(info.slot, sepKeyOut, info.length);
   if ( info.trunc ) {// TODO: ??
      u8 *k = isLarge(info.slot + 1) ? getRestLarge(info.slot + 1) : getRest(info.slot + 1);
      sepKeyOut[info.length - 1] = k[info.length - prefix_length - sizeof(SketchType) - 1];
   }
}
// -------------------------------------------------------------------------------------
bool BTreeNode::sanityCheck(u8 *key, unsigned int keyLength)
{
   // Lower Bound exclusive, upper bound inclusive
   bool res = true;
   if ( lower_fence.offset ) {
      int cmp = cmpKeys(key, getLowerFenceKey(), keyLength, lower_fence.length);
      res &= cmp > 0;
   }
   if ( upper_fence.offset ) {
      int cmp = cmpKeys(key, getUpperFenceKey(), keyLength, upper_fence.length);
      res &= cmp <= 0;
   }
   return res;
}
// -------------------------------------------------------------------------------------
Swip<BTreeNode> &BTreeNode::lookupInner(u8 *key, unsigned keyLength)
{
   s32 pos = lowerBound<false>(key, keyLength);
   if ( pos == count )
      return upper;
   return getValue(pos);
}
// -------------------------------------------------------------------------------------
void BTreeNode::split(WritePageGuard<BTreeNode> &parent, WritePageGuard<BTreeNode> &nodeLeft, unsigned sepSlot, u8 *sepKey, unsigned sepLength)
{
   //PRE: current, parent and nodeLeft are x locked
   assert(sepSlot > 0);
   assert(sepSlot < (EFFECTIVE_PAGE_SIZE / sizeof(ValueType)));
   // -------------------------------------------------------------------------------------
   nodeLeft->setFences(getLowerFenceKey(), lower_fence.length, sepKey, sepLength);
   BTreeNode tmp(is_leaf);
   BTreeNode *nodeRight = &tmp;
   nodeRight->setFences(sepKey, sepLength, getUpperFenceKey(), upper_fence.length);
   bool succ = parent->insert(sepKey, sepLength, nodeLeft.swip());
   static_cast<void>(succ);
   assert(succ);
   if ( is_leaf ) {
      copyKeyValueRange(nodeLeft.ptr(), 0, 0, sepSlot + 1);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
   } else {
      copyKeyValueRange(nodeLeft.ptr(), 0, 0, sepSlot);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
      nodeLeft->upper = getValue(nodeLeft->count);
      nodeRight->upper = upper;
   }
   nodeLeft->makeHint();
   nodeRight->makeHint();
   memcpy(reinterpret_cast<char *>(this), nodeRight, sizeof(BTreeNode));
}
// -------------------------------------------------------------------------------------
bool BTreeNode::removeSlot(u16 slotId)
{
   // TODO: check
   if ( slot[slotId].rest_len )
      space_used -= sizeof(ValueType) + (isLarge(slotId) ? (getRestLenLarge(slotId) + sizeof(u16)) : getRestLen(slotId));
   space_used -= (is_leaf) ? getPayloadLength(slotId) : 0;
   memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
   count--;
   makeHint();
   return true;
}
// -------------------------------------------------------------------------------------
bool BTreeNode::remove(u8 *key, unsigned keyLength)
{
   int slotId = lowerBound<true>(key, keyLength);
   if ( slotId == -1 )
      return false; // key not found
   return removeSlot(slotId);
}
// -------------------------------------------------------------------------------------
}
}
}