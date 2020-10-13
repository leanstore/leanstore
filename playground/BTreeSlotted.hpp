#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <x86intrin.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
struct BTreeNode;
using ValueType = BTreeNode*;
using SketchType = u32;
// -------------------------------------------------------------------------------------
static inline u64 swap(u64 x)
{
   return __builtin_bswap64(x);
}
static inline u32 swap(u32 x)
{
   return __builtin_bswap32(x);
}
static inline u16 swap(u16 x)
{
   return __builtin_bswap16(x);
}
// -------------------------------------------------------------------------------------
struct BTreeNodeHeader {
   static const unsigned pageSize = 16 * 1024;
   static const unsigned underFullSize = pageSize * 0.6;

   struct FenceKey {
      u16 offset;
      u16 length;
   };

   BTreeNode* upper = nullptr;
   FenceKey lowerFence = {0, 0};
   FenceKey upperFence = {0, 0};

   u16 count = 0;
   bool isLeaf;
   u16 spaceUsed = 0;
   u16 dataOffset = static_cast<u16>(pageSize);
   u16 prefixLength = 0;

   static const unsigned hintCount = 16;
   u32 hint[hintCount];

   BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
   ~BTreeNodeHeader() {}

   inline u8* ptr() { return reinterpret_cast<u8*>(this); }
   inline bool isInner() { return !isLeaf; }
   inline u8* getLowerFenceKey() { return lowerFence.offset ? ptr() + lowerFence.offset : nullptr; }
   inline u8* getUpperFenceKey() { return upperFence.offset ? ptr() + upperFence.offset : nullptr; }
};
// -------------------------------------------------------------------------------------
struct BTreeNode : public BTreeNodeHeader {
   struct Slot {
      u16 offset;
      u8 headLen;
      u8 restLen;
      union {
         SketchType sketch;
         u8 sketchBytes[4];
      };
   };
   Slot slot[(pageSize - sizeof(BTreeNodeHeader)) / (sizeof(Slot))];

   BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) {}

   unsigned freeSpace() { return dataOffset - (reinterpret_cast<u8*>(slot + count) - ptr()); }
   unsigned freeSpaceAfterCompaction() { return pageSize - (reinterpret_cast<u8*>(slot + count) - ptr()) - spaceUsed; }

   bool requestSpaceFor(unsigned spaceNeeded)
   {
      if (spaceNeeded <= freeSpace())
         return true;
      if (spaceNeeded <= freeSpaceAfterCompaction()) {
         compactify();
         return true;
      }
      return false;
   }

   static BTreeNode* makeLeaf() { return new BTreeNode(true); }
   static BTreeNode* makeInner() { return new BTreeNode(false); }

   // Accessors for normal strings: | Value | restKey | Payload
   inline u8* getRest(unsigned slotId)
   {
      assert(!isLarge(slotId));
      return ptr() + slot[slotId].offset + sizeof(ValueType);
   }
   inline unsigned getRestLen(unsigned slotId)
   {
      assert(!isLarge(slotId));
      return slot[slotId].restLen;
   }
   // AAA
   inline u8* getPayload(unsigned slotId)
   {
      assert(isLeaf);
      assert(!isLarge(slotId));
      return ptr() + slot[slotId].offset + sizeof(ValueType) + getRestLen(slotId);
   }

   // Accessors for large strings: | Value | restLength | restKey | Payload
   static constexpr u8 largeLimit = 254;
   static constexpr u8 largeMarker = largeLimit + 1;
   inline u8* getRestLarge(unsigned slotId)
   {
      assert(isLarge(slotId));
      return ptr() + slot[slotId].offset + sizeof(ValueType) + sizeof(u16);
   }
   inline u16& getRestLenLarge(unsigned slotId)
   {
      assert(isLarge(slotId));
      return *reinterpret_cast<u16*>(ptr() + slot[slotId].offset + sizeof(ValueType));
   }
   inline bool isLarge(unsigned slotId) { return slot[slotId].restLen == largeMarker; }
   inline void setLarge(unsigned slotId) { slot[slotId].restLen = largeMarker; }
   // AAA
   inline u8* getPayloadLarge(unsigned slotId)
   {
      assert(isLeaf);
      assert(isLarge(slotId));
      return ptr() + slot[slotId].offset + sizeof(ValueType) + sizeof(u16) + getRestLenLarge(slotId);
   }

   // Accessors for both types of strings
   inline u64 getPayloadLength(unsigned slotId) { return *reinterpret_cast<u64*>(ptr() + slot[slotId].offset); }
   inline ValueType& getValue(unsigned slotId) { return *reinterpret_cast<ValueType*>(ptr() + slot[slotId].offset); }
   inline unsigned getFullKeyLength(unsigned slotId)
   {
      return prefixLength + slot[slotId].headLen + (isLarge(slotId) ? getRestLenLarge(slotId) : getRestLen(slotId));
   }
   inline void copyFullKey(unsigned slotId, u8* out, unsigned fullLength)
   {
      memcpy(out, getLowerFenceKey(), prefixLength);
      out += prefixLength;
      fullLength -= prefixLength;
      switch (slot[slotId].headLen) {
         case 4:
            *reinterpret_cast<u32*>(out) = swap(slot[slotId].sketch);
            memcpy(out + slot[slotId].headLen, (isLarge(slotId) ? getRestLarge(slotId) : getRest(slotId)), fullLength - slot[slotId].headLen);
            break;
         case 3:
            out[2] = slot[slotId].sketchBytes[1];  // fallthrough
         case 2:
            out[1] = slot[slotId].sketchBytes[2];  // fallthrough
         case 1:
            out[0] = slot[slotId].sketchBytes[3];  // fallthrough
         case 0:
            break;
         default:
            __builtin_unreachable();
      };
   }

   static unsigned spaceNeeded(unsigned keyLength, unsigned prefixLength)
   {
      assert(keyLength >= prefixLength);
      unsigned restLen = keyLength - prefixLength;
      if (restLen <= 4)
         return sizeof(Slot) + sizeof(ValueType);
      restLen -= sizeof(SketchType);
      return sizeof(Slot) + restLen + sizeof(ValueType) + ((restLen > largeLimit) ? sizeof(u16) : 0);
   }

   static int cmpKeys(u8* a, u8* b, unsigned aLength, unsigned bLength)
   {
      int c = memcmp(a, b, min(aLength, bLength));
      if (c)
         return c;
      return (aLength - bLength);
   }

   static SketchType head(u8*& key, unsigned& keyLength)
   {
      SketchType result;
      if (keyLength > 3) {
         result = swap(*reinterpret_cast<u32*>(key));
         key += sizeof(SketchType);
         keyLength -= sizeof(SketchType);
         return result;
      }
      switch (keyLength) {
         case 0:
            result = 0;
            break;
         case 1:
            result = static_cast<u32>(key[0]) << 24;
            break;
         case 2:
            result = static_cast<u32>(swap(*reinterpret_cast<u16*>(key))) << 16;
            break;
         case 3:
            result = (static_cast<u32>(swap(*reinterpret_cast<u16*>(key))) << 16) | (static_cast<u32>(key[2]) << 8);
            break;
         default:
            __builtin_unreachable();
      }
      key += keyLength;  // should not be needed
      keyLength = 0;
      return result;
   }

   void makeHint()
   {
      unsigned dist = count / (hintCount + 1);
      for (unsigned i = 0; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].sketch;
   }

   template <bool equalityOnly = false>
   unsigned lowerBound(u8* key, unsigned keyLength)
   {
      // for (unsigned i=1; i<count; i++)
      // assert(slot[i-1].sketch <= slot[i].sketch);

      if (lowerFence.offset)
         assert(cmpKeys(key, getLowerFenceKey(), keyLength, lowerFence.length) > 0);
      if (upperFence.offset)
         assert(cmpKeys(key, getUpperFenceKey(), keyLength, upperFence.length) <= 0);

      if (equalityOnly) {
         if ((keyLength < prefixLength) || (bcmp(key, getLowerFenceKey(), prefixLength) != 0))
            return -1;
      } else {
         int prefixCmp = cmpKeys(key, getLowerFenceKey(), min<unsigned>(keyLength, prefixLength), prefixLength);
         if (prefixCmp < 0)
            return 0;
         else if (prefixCmp > 0)
            return count;
      }
      key += prefixLength;
      keyLength -= prefixLength;

      unsigned lower = 0;
      unsigned upper = count;

      unsigned oldKeyLength = keyLength;
      SketchType keyHead = head(key, keyLength);

      if (count > hintCount * 2) {
         unsigned dist = count / (hintCount + 1);
         unsigned pos;
         for (pos = 0; pos < hintCount; pos++)
            if (hint[pos] >= keyHead)
               break;
         lower = pos * dist;
         unsigned pos2;
         for (pos2 = pos; pos2 < hintCount; pos2++)
            if (hint[pos2] != keyHead)
               break;
         if (pos2 < hintCount)
            upper = (pos2 + 1) * dist;
         // cout << isLeaf << " " << count << " " << lower << " " << upper << " " << dist << endl;
      }

      while (lower < upper) {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (keyHead < slot[mid].sketch) {
            upper = mid;
         } else if (keyHead > slot[mid].sketch) {
            lower = mid + 1;
         } else if (slot[mid].restLen == 0) {
            if (oldKeyLength < slot[mid].headLen) {
               upper = mid;
            } else if (oldKeyLength > slot[mid].headLen) {
               lower = mid + 1;
            } else {
               return mid;
            }
         } else {
            int cmp;
            if (isLarge(mid)) {
               cmp = cmpKeys(key, getRestLarge(mid), keyLength, getRestLenLarge(mid));
            } else {
               cmp = cmpKeys(key, getRest(mid), keyLength, getRestLen(mid));
            }
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               return mid;
            }
         }
      }
      if (equalityOnly)
         return -1;
      return lower;
   }

   void updateHint(unsigned slotId)
   {
      unsigned dist = count / (hintCount + 1);
      unsigned begin = 0;
      if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
         begin = (slotId / dist) - 1;
      for (unsigned i = begin; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].sketch;
      for (unsigned i = 0; i < hintCount; i++)
         assert(hint[i] == slot[dist * (i + 1)].sketch);
   }
   // -------------------------------------------------------------------------------------
   bool insert(u8* key, unsigned keyLength, ValueType value, u8* payload = nullptr)
   {
      const u16 space_needed = (isLeaf) ? u64(value) + spaceNeeded(keyLength, prefixLength) : spaceNeeded(keyLength, prefixLength);
      if (!requestSpaceFor(space_needed))
         return false;  // no space, insert fails
      unsigned slotId = lowerBound<false>(key, keyLength);
      memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
      storeKeyValue(slotId, key, keyLength, value, payload);
      count++;
      updateHint(slotId);
      assert(lowerBound<true>(key, keyLength) == slotId);
      return true;
   }
   // -------------------------------------------------------------------------------------
   bool removeSlot(unsigned slotId)
   {
      if (slot[slotId].restLen)
         spaceUsed -= sizeof(ValueType) + (isLarge(slotId) ? (getRestLenLarge(slotId) + sizeof(u16)) : slot[slotId].restLen);
      spaceUsed -= getPayloadLength(slotId);
      memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
      count--;
      makeHint();
      return true;
   }

   bool remove(u8* key, unsigned keyLength)
   {
      int slotId = lowerBound<true>(key, keyLength);
      if (slotId == -1)
         return false;  // key not found
      return removeSlot(slotId);
   }

   void compactify()
   {
      unsigned should = freeSpaceAfterCompaction();
      static_cast<void>(should);
      BTreeNode tmp(isLeaf);
      tmp.setFences(getLowerFenceKey(), lowerFence.length, getUpperFenceKey(), upperFence.length);
      copyKeyValueRange(&tmp, 0, 0, count);
      tmp.upper = upper;
      memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(BTreeNode));
      makeHint();
      assert(freeSpace() == should);
   }

   // merge right node into this node
   bool merge(unsigned slotId, BTreeNode* parent, BTreeNode* right)
   {
      if (isLeaf) {
         assert(right->isLeaf);
         assert(parent->isInner());
         BTreeNode tmp(isLeaf);
         tmp.setFences(getLowerFenceKey(), lowerFence.length, right->getUpperFenceKey(), right->upperFence.length);
         unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
         unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
         unsigned spaceUpperBound =
             spaceUsed + right->spaceUsed + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
         if (spaceUpperBound > pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<u8*>(right), &tmp, sizeof(BTreeNode));
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
         unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow +
                                    rightGrow + spaceNeeded(extraKeyLength, tmp.prefixLength);
         if (spaceUpperBound > pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         u8 extraKey[extraKeyLength];
         parent->copyFullKey(slotId, extraKey, extraKeyLength);
         storeKeyValue(count, extraKey, extraKeyLength, parent->getValue(slotId));
         count++;
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<u8*>(right), &tmp, sizeof(BTreeNode));
         return true;
      }
   }

   // store key/value pair at slotId
   void storeKeyValue(u16 slotId, u8* key, unsigned keyLength, ValueType value, u8* payload = nullptr)
   {
      // Head
      key += prefixLength;
      keyLength -= prefixLength;
      slot[slotId].headLen = (keyLength >= sizeof(SketchType)) ? sizeof(SketchType) : keyLength;
      slot[slotId].sketch = head(key, keyLength);
      // Value
      unsigned space = keyLength + sizeof(ValueType) + ((keyLength > largeLimit) ? sizeof(u16) : 0) + ((isLeaf) ? u64(value) : 0);
      dataOffset -= space;
      spaceUsed += space;
      slot[slotId].offset = dataOffset;
      getValue(slotId) = value;
      // Rest
      if (keyLength > largeLimit) {  // large string
         setLarge(slotId);
         getRestLenLarge(slotId) = keyLength;
         memcpy(getRestLarge(slotId), key, keyLength);
         if (isLeaf) {  // AAA
            assert(payload != nullptr);
            memcpy(getPayloadLarge(slotId), payload, u64(value));
         }
      } else {  // normal string
         slot[slotId].restLen = keyLength;
         memcpy(getRest(slotId), key, keyLength);
         if (isLeaf) {  // AAA
            assert(payload != nullptr);
            memcpy(getPayload(slotId), payload, u64(value));
         }
      }
      // store payload
   }

   void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, unsigned count)
   {
      if (prefixLength == dst->prefixLength) {
         // Fast path
         memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
         for (unsigned i = 0; i < count; i++) {
            unsigned space = sizeof(ValueType) + (isLarge(srcSlot + i) ? (getRestLenLarge(srcSlot + i) + sizeof(u16)) : getRestLen(srcSlot + i));
            if (dst->isLeaf) {
               space += getPayloadLength(srcSlot);  // AAA: Payload size
            }
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot + i].offset = dst->dataOffset;
            memcpy(reinterpret_cast<u8*>(dst) + dst->dataOffset, ptr() + slot[srcSlot + i].offset, space);
         }
      } else {
         for (unsigned i = 0; i < count; i++)
            copyKeyValue(srcSlot + i, dst, dstSlot + i);
      }
      dst->count += count;
      assert((ptr() + dst->dataOffset) >= reinterpret_cast<u8*>(slot + count));
   }

   void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot)
   {
      unsigned fullLength = getFullKeyLength(srcSlot);
      u8 key[fullLength];
      copyFullKey(srcSlot, key, fullLength);
      dst->storeKeyValue(dstSlot, key, fullLength, getValue(srcSlot), (isLarge(srcSlot) ? getPayloadLarge(srcSlot) : getPayload(srcSlot)));
   }

   void insertFence(FenceKey& fk, u8* key, unsigned keyLength)
   {
      if (!key)
         return;
      assert(freeSpace() >= keyLength);
      dataOffset -= keyLength;
      spaceUsed += keyLength;
      fk.offset = dataOffset;
      fk.length = keyLength;
      memcpy(ptr() + dataOffset, key, keyLength);
   }
   // -------------------------------------------------------------------------------------
   void setFences(u8* lowerKey, unsigned lowerLen, u8* upperKey, unsigned upperLen)
   {
      insertFence(lowerFence, lowerKey, lowerLen);
      insertFence(upperFence, upperKey, upperLen);
      for (prefixLength = 0; (prefixLength < min(lowerLen, upperLen)) && (lowerKey[prefixLength] == upperKey[prefixLength]); prefixLength++)
         ;
   }
   // -------------------------------------------------------------------------------------
   void split(BTreeNode* parent, unsigned sepSlot, u8* sepKey, unsigned sepLength)
   {
      assert(sepSlot > 0);
      assert(sepSlot < (BTreeNodeHeader::pageSize / sizeof(ValueType)));
      BTreeNode* nodeLeft = new BTreeNode(isLeaf);
      nodeLeft->setFences(getLowerFenceKey(), lowerFence.length, sepKey, sepLength);
      BTreeNode tmp(isLeaf);
      BTreeNode* nodeRight = &tmp;
      nodeRight->setFences(sepKey, sepLength, getUpperFenceKey(), upperFence.length);
      bool succ = parent->insert(sepKey, sepLength, nodeLeft);
      static_cast<void>(succ);
      assert(succ);
      if (isLeaf) {
         copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
         copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
      } else {
         copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
         copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
         nodeLeft->upper = getValue(nodeLeft->count);
         nodeRight->upper = upper;
      }
      nodeLeft->makeHint();
      nodeRight->makeHint();
      memcpy(reinterpret_cast<char*>(this), nodeRight, sizeof(BTreeNode));
   }

   struct SeparatorInfo {
      unsigned length;
      unsigned slot;
      bool trunc;  // TODO: ???
   };

   unsigned commonPrefix(unsigned aPos, unsigned bPos)
   {
      assert(aPos < count);
      assert(bPos < count);
      if ((slot[aPos].sketch == slot[bPos].sketch) && (slot[aPos].headLen == slot[bPos].headLen)) {
         unsigned aLen, bLen;
         u8 *a, *b;
         if (isLarge(aPos)) {
            a = getRestLarge(aPos);
            aLen = getRestLenLarge(aPos);
         } else {
            a = getRest(aPos);
            aLen = getRestLen(aPos);
         }
         if (isLarge(bPos)) {
            b = getRestLarge(bPos);
            bLen = getRestLenLarge(bPos);
         } else {
            b = getRest(bPos);
            bLen = getRestLen(bPos);
         }
         unsigned i;
         for (i = 0; i < min(aLen, bLen); i++)
            if (a[i] != b[i])
               break;
         return i + slot[aPos].headLen;
      }
      unsigned limit = min(slot[aPos].headLen, slot[bPos].headLen);
      unsigned i;
      for (i = 0; i < limit; i++)
         if (slot[aPos].sketchBytes[3 - i] != slot[bPos].sketchBytes[3 - i])
            return i;
      return i;
   }

   SeparatorInfo findSep()
   {
      if (isInner())
         return SeparatorInfo{getFullKeyLength(count / 2), static_cast<unsigned>(count / 2), false};

      unsigned lower = count / 2 - count / 16;
      unsigned upper = count / 2 + count / 16;
      assert(upper < count);
      unsigned maxPos = count / 2;
      int maxPrefix = commonPrefix(maxPos, 0);
      for (unsigned i = lower; i < upper; i++) {
         int prefix = commonPrefix(i, 0);
         if (prefix > maxPrefix) {
            maxPrefix = prefix;
            maxPos = i;
         }
      }
      unsigned common = commonPrefix(maxPos, maxPos + 1);
      if ((common > sizeof(SketchType)) && (getFullKeyLength(maxPos) - prefixLength > common) &&
          (getFullKeyLength(maxPos + 1) - prefixLength > common + 2)) {
         return SeparatorInfo{static_cast<unsigned>(prefixLength + common + 1), maxPos, true};
      }
      return SeparatorInfo{getFullKeyLength(maxPos), maxPos, false};
   }

   void getSep(u8* sepKeyOut, SeparatorInfo info)
   {
      copyFullKey(info.slot, sepKeyOut, info.length);
      if (info.trunc) {  // TODO: ??
         u8* k = isLarge(info.slot + 1) ? getRestLarge(info.slot + 1) : getRest(info.slot + 1);
         sepKeyOut[info.length - 1] = k[info.length - prefixLength - sizeof(SketchType) - 1];
      }
   }

   BTreeNode* lookupInner(u8* key, unsigned keyLength)
   {
      unsigned pos = lowerBound<false>(key, keyLength);
      if (pos == count)
         return upper;
      return getValue(pos);
   }

   void destroy()
   {
      if (isInner()) {
         for (unsigned i = 0; i < count; i++)
            getValue(i)->destroy();
         upper->destroy();
      }
      delete this;
      return;
   }
};
// -------------------------------------------------------------------------------------
static_assert(sizeof(BTreeNode) == BTreeNodeHeader::pageSize, "page size problem");
// -------------------------------------------------------------------------------------
struct BTree {
   BTreeNode* root;
   BTree();
   bool lookup(u8* key, unsigned keyLength, u64& payloadLength, u8* result);
   void lookupInner(u8* key, unsigned keyLength);
   void splitNode(BTreeNode* node, BTreeNode* parent, u8* key, unsigned keyLength);
   void ensureSpace(BTreeNode* toSplit, unsigned spaceNeeded, u8* key, unsigned keyLength);
   void insert(u8* key, unsigned keyLength, u64 payloadLength, u8* payload = nullptr);
   bool remove(u8* key, unsigned keyLength);
   ~BTree();
};
// -------------------------------------------------------------------------------------
unsigned countInner(BTreeNode* node);
unsigned countPages(BTreeNode* node);
unsigned bytesFree(BTreeNode* node);
unsigned height(BTreeNode* node);
void printInfos(BTreeNode* root, uint64_t totalSize);
// -------------------------------------------------------------------------------------
