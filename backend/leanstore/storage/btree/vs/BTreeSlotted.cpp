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
   unsigned dist = count / (hintCount + 1);
   for ( unsigned i = 0; i < hintCount; i++ )
      hint[i] = slot[dist * (i + 1)].sketch;
}
// -------------------------------------------------------------------------------------
void BTreeNode::updateHint(unsigned slotId)
{
   unsigned dist = count / (hintCount + 1);
   unsigned begin = 0;
   if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
      begin = (slotId / dist) - 1;
   for ( unsigned i = begin; i < hintCount; i++ )
      hint[i] = slot[dist * (i + 1)].sketch;
   for ( unsigned i = 0; i < hintCount; i++ )
      assert(hint[i] == slot[dist * (i + 1)].sketch);
}
// -------------------------------------------------------------------------------------
bool BTreeNode::insert(u8 *key, unsigned keyLength, ValueType value, u8 *payload)
{
   const u16 space_needed = (isLeaf) ? value.raw() + spaceNeeded(keyLength, prefixLength) : spaceNeeded(keyLength, prefixLength);
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
   BTreeNode tmp(isLeaf);
   tmp.setFences(getLowerFenceKey(), lowerFence.length, getUpperFenceKey(), upperFence.length);
   copyKeyValueRange(&tmp, 0, 0, count);
   tmp.upper = upper;
   memcpy(reinterpret_cast<char *>(this), &tmp, sizeof(BTreeNode));
   makeHint();
   assert(freeSpace() == should);
}
// -------------------------------------------------------------------------------------
bool BTreeNode::merge(unsigned slotId, BTreeNode *parent, BTreeNode *right)
{
   if ( isLeaf ) {
      assert(right->isLeaf);
      assert(parent->isInner());
      BTreeNode tmp(isLeaf);
      tmp.setFences(getLowerFenceKey(), lowerFence.length, right->getUpperFenceKey(), right->upperFence.length);
      unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
      unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
      unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<u8 *>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
      if ( spaceUpperBound > EFFECTIVE_PAGE_SIZE )
         return false;
      copyKeyValueRange(&tmp, 0, 0, count);
      right->copyKeyValueRange(&tmp, count, 0, right->count);
      parent->removeSlot(slotId);
      memcpy(reinterpret_cast<u8 *>(right), &tmp, sizeof(BTreeNode));
      right->makeHint();
      return true;
   } else {
      assert(!right->isLeaf);
      assert(parent->isInner());
      BTreeNode tmp(isLeaf);
      tmp.setFences(getLowerFenceKey(), lowerFence.length, right->getUpperFenceKey(), right->upperFence.length);
      unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
      unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
      unsigned extraKeyLength = parent->getFullKeyLength(slotId);
      unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<u8 *>(slot + count + right->count) - ptr()) + leftGrow + rightGrow + spaceNeeded(extraKeyLength, tmp.prefixLength);
      if ( spaceUpperBound > EFFECTIVE_PAGE_SIZE )
         return false;
      copyKeyValueRange(&tmp, 0, 0, count);
      u8 extraKey[extraKeyLength];
      parent->copyFullKey(slotId, extraKey, extraKeyLength);
      storeKeyValue(count, extraKey, extraKeyLength, parent->getValue(slotId));
      count++;
      right->copyKeyValueRange(&tmp, count, 0, right->count);
      parent->removeSlot(slotId);
      memcpy(reinterpret_cast<u8 *>(right), &tmp, sizeof(BTreeNode));
      return true;
   }
}
// -------------------------------------------------------------------------------------
void BTreeNode::storeKeyValue(u16 slotId, u8 *key, unsigned keyLength, ValueType value, u8 *payload)
{

   // Head
   key += prefixLength;
   keyLength -= prefixLength;
   slot[slotId].headLen = (keyLength >= sizeof(SketchType)) ? sizeof(SketchType) : keyLength;
   slot[slotId].sketch = head(key, keyLength);
   // Value
   unsigned space = keyLength + sizeof(ValueType) + ((keyLength > largeLimit) ? sizeof(u16) : 0) + ((isLeaf) ? value.raw() : 0);
   dataOffset -= space;
   spaceUsed += space;
   slot[slotId].offset = dataOffset;
   getValue(slotId) = value;
   // Rest
   if ( keyLength > largeLimit ) { // large string
      setLarge(slotId);
      getRestLenLarge(slotId) = keyLength;
      memcpy(getRestLarge(slotId), key, keyLength);
      if ( isLeaf ) { // AAA
         assert(payload != nullptr);
         memcpy(getPayloadLarge(slotId), payload, getPayloadLength(slotId));
      }
   } else { // normal string
      slot[slotId].restLen = keyLength;
      memcpy(getRest(slotId), key, keyLength);
      if ( isLeaf ) { // AAA
         assert(payload != nullptr);
         memcpy(getPayload(slotId), payload, getPayloadLength(slotId));
      }
   }
   // store payload
}
// -------------------------------------------------------------------------------------
void BTreeNode::copyKeyValueRange(BTreeNode *dst, u16 dstSlot, u16 srcSlot, unsigned count)
{
   if ( prefixLength == dst->prefixLength ) {
      // Fast path
      memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
      for ( unsigned i = 0; i < count; i++ ) {
         unsigned space = sizeof(ValueType) + (isLarge(srcSlot + i) ? (getRestLenLarge(srcSlot + i) + sizeof(u16)) : getRestLen(srcSlot + i));
         if ( dst->isLeaf ) {
            assert(isLeaf);
            space += getPayloadLength(srcSlot + i); // AAA: Payload size
         }
         dst->dataOffset -= space;
         dst->spaceUsed += space;
         dst->slot[dstSlot + i].offset = dst->dataOffset;
         memcpy(reinterpret_cast<u8 *>(dst) + dst->dataOffset, ptr() + slot[srcSlot + i].offset, space);
      }
   } else {
      for ( unsigned i = 0; i < count; i++ )
         copyKeyValue(srcSlot + i, dst, dstSlot + i);
   }
   dst->count += count;
   assert((ptr() + dst->dataOffset) >= reinterpret_cast<u8 *>(slot + count));
}
// -------------------------------------------------------------------------------------
void BTreeNode::copyKeyValue(u16 srcSlot, BTreeNode *dst, u16 dstSlot)
{
   unsigned fullLength = getFullKeyLength(srcSlot);
   u8 key[fullLength];
   copyFullKey(srcSlot, key, fullLength);
   u8 *payload = (isLeaf) ? ((isLarge(srcSlot) ? getPayloadLarge(srcSlot) : getPayload(srcSlot))) : nullptr;
   dst->storeKeyValue(dstSlot, key, fullLength, getValue(srcSlot), payload);
}
// -------------------------------------------------------------------------------------
void BTreeNode::insertFence(BTreeNodeHeader::FenceKey &fk, u8 *key, unsigned keyLength)
{
   if ( !key )
      return;
   assert(freeSpace() >= keyLength);
   dataOffset -= keyLength;
   spaceUsed += keyLength;
   fk.offset = dataOffset;
   fk.length = keyLength;
   memcpy(ptr() + dataOffset, key, keyLength);
}
// -------------------------------------------------------------------------------------
void BTreeNode::setFences(u8 *lowerKey, unsigned lowerLen, u8 *upperKey, unsigned upperLen)
{
   insertFence(lowerFence, lowerKey, lowerLen);
   insertFence(upperFence, upperKey, upperLen);
   for ( prefixLength = 0; (prefixLength < min(lowerLen, upperLen)) && (lowerKey[prefixLength] == upperKey[prefixLength]); prefixLength++ );
}
// -------------------------------------------------------------------------------------
unsigned BTreeNode::commonPrefix(unsigned aPos, unsigned bPos)
{
   assert(aPos < count);
   assert(bPos < count);
   if ((slot[aPos].sketch == slot[bPos].sketch) && (slot[aPos].headLen == slot[bPos].headLen)) {
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
      return i + slot[aPos].headLen;
   }
   unsigned limit = min(slot[aPos].headLen, slot[bPos].headLen);
   unsigned i;
   for ( i = 0; i < limit; i++ )
      if ( slot[aPos].sketchBytes[3 - i] != slot[bPos].sketchBytes[3 - i] )
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
   assert(upper < count);
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
   if ((common > sizeof(SketchType)) && (getFullKeyLength(maxPos) - prefixLength > common) && (getFullKeyLength(maxPos + 1) - prefixLength > common + 2)) {
      return SeparatorInfo{static_cast<unsigned>(prefixLength + common + 1), maxPos, true};
   }
   return SeparatorInfo{getFullKeyLength(maxPos), maxPos, false};
}
// -------------------------------------------------------------------------------------
void BTreeNode::getSep(u8 *sepKeyOut, BTreeNodeHeader::SeparatorInfo info)
{
   copyFullKey(info.slot, sepKeyOut, info.length);
   if ( info.trunc ) {// TODO: ??
      u8 *k = isLarge(info.slot + 1) ? getRestLarge(info.slot + 1) : getRest(info.slot + 1);
      sepKeyOut[info.length - 1] = k[info.length - prefixLength - sizeof(SketchType) - 1];
   }
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
   nodeLeft->setFences(getLowerFenceKey(), lowerFence.length, sepKey, sepLength);
   BTreeNode tmp(isLeaf);
   BTreeNode *nodeRight = &tmp;
   nodeRight->setFences(sepKey, sepLength, getUpperFenceKey(), upperFence.length);
   bool succ = parent->insert(sepKey, sepLength, nodeLeft.swip());
   static_cast<void>(succ);
   assert(succ);
   if ( isLeaf ) {
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
bool BTreeNode::removeSlot(unsigned slotId)
{
   // TOOD: check
   if ( slot[slotId].restLen )
      spaceUsed -= sizeof(ValueType) + (isLarge(slotId) ? (getRestLenLarge(slotId) + sizeof(u16)) : getRestLen(slotId));
   spaceUsed -= (isLeaf) ? getPayloadLength(slotId) : 0;
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