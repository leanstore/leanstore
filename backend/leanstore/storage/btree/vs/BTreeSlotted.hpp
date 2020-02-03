#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
// -------------------------------------------------------------------------------------
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
using namespace leanstore::buffermanager;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace btree
{
namespace vs
{
// -------------------------------------------------------------------------------------
struct BTreeNode;
using ValueType = Swip<BTreeNode>;
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
  static const unsigned underFullSize = EFFECTIVE_PAGE_SIZE * 0.6;
  static const unsigned K_WAY_MERGE_THRESHOLD = EFFECTIVE_PAGE_SIZE * 0.45;

  struct SeparatorInfo {
    unsigned length;
    unsigned slot;
    bool trunc;  // TODO: ???
  };

  struct FenceKey {
    u16 offset;
    u16 length;
  };

  Swip<BTreeNode> upper = nullptr;
  FenceKey lower_fence = {0, 0};
  FenceKey upper_fence = {0, 0};

  u16 count = 0;  // count number of separators, excluding the upper swip
  bool is_leaf;
  u16 space_used = 0;  // does not include the header
  u16 data_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);
  u16 prefix_length = 0;

  static const unsigned hint_count = 16;
  u32 hint[hint_count];

  BTreeNodeHeader(bool is_leaf) : is_leaf(is_leaf) {}
  ~BTreeNodeHeader() {}

  inline u8* ptr() { return reinterpret_cast<u8*>(this); }
  inline bool isInner() { return !is_leaf; }
  inline u8* getLowerFenceKey() { return lower_fence.offset ? ptr() + lower_fence.offset : nullptr; }
  inline u8* getUpperFenceKey() { return upper_fence.offset ? ptr() + upper_fence.offset : nullptr; }
  inline bool isUpperFenceInfinity() { return !upper_fence.offset; };
  inline bool isLowerFenceInfinity() { return !lower_fence.offset; };
};
// -------------------------------------------------------------------------------------
struct BTreeNode : public BTreeNodeHeader {
  struct Slot {
    u16 offset;
    u8 head_len;
    u8 rest_len;
    union {
      SketchType sketch;
      u8 sketch_bytes[4];
    };
  };
  Slot slot[(EFFECTIVE_PAGE_SIZE - sizeof(BTreeNodeHeader)) / (sizeof(Slot))];

  BTreeNode(bool is_leaf) : BTreeNodeHeader(is_leaf) {}

  unsigned freeSpace() { return data_offset - (reinterpret_cast<u8*>(slot + count) - ptr()); }
  unsigned freeSpaceAfterCompaction() { return EFFECTIVE_PAGE_SIZE - (reinterpret_cast<u8*>(slot + count) - ptr()) - space_used; }
  // -------------------------------------------------------------------------------------
  double fillFactor(){return (1 - (freeSpaceAfterCompaction() * 1.0 / EFFECTIVE_PAGE_SIZE)); }
  // -------------------------------------------------------------------------------------

  bool hasEnoughSpaceFor(u32 space_needed) { return (space_needed <= freeSpace() || space_needed <= freeSpaceAfterCompaction()); }
  // ATTENTION: this method has side effects !
  bool requestSpaceFor(unsigned space_needed)
  {
    if (space_needed <= freeSpace())
      return true;
    if (space_needed <= freeSpaceAfterCompaction()) {
      compactify();
      return true;
    }
    return false;
  }

  // Accessors for normal strings: | Value | restKey | Payload
  inline u8* getData(unsigned slotId) { return ptr() + slot[slotId].offset; }
  inline u8* getRest(unsigned slotId)
  {
    assert(!isLarge(slotId));
    return ptr() + slot[slotId].offset + sizeof(ValueType);
  }
  inline unsigned getRestLen(unsigned slotId)
  {
    assert(!isLarge(slotId));
    return slot[slotId].rest_len;
  }
  // AAA
  inline u8* getPayload(unsigned slotId)
  {
    assert(is_leaf);
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
  inline bool isLarge(unsigned slotId) { return slot[slotId].rest_len == largeMarker; }
  inline void setLarge(unsigned slotId) { slot[slotId].rest_len = largeMarker; }
  // AAA
  inline u8* getPayloadLarge(unsigned slotId)
  {
    assert(is_leaf);
    assert(isLarge(slotId));
    return ptr() + slot[slotId].offset + sizeof(ValueType) + sizeof(u16) + getRestLenLarge(slotId);
  }

  // Accessors for both types of strings
  inline u16 getPayloadLength(unsigned slotId) { return *reinterpret_cast<u16*>(ptr() + slot[slotId].offset); }
  inline ValueType& getValue(unsigned slotId) { return *reinterpret_cast<ValueType*>(ptr() + slot[slotId].offset); }
  inline unsigned getFullKeyLength(unsigned slotId)
  {
    return prefix_length + slot[slotId].head_len + (isLarge(slotId) ? getRestLenLarge(slotId) : getRestLen(slotId));
  }
  inline void copyFullKey(unsigned slotId, u8* out, unsigned fullLength)
  {
    memcpy(out, getLowerFenceKey(), prefix_length);
    out += prefix_length;
    fullLength -= prefix_length;
    switch (slot[slotId].head_len) {
      case 4:
        *reinterpret_cast<u32*>(out) = swap(slot[slotId].sketch);
        memcpy(out + slot[slotId].head_len, (isLarge(slotId) ? getRestLarge(slotId) : getRest(slotId)), fullLength - slot[slotId].head_len);
        break;
      case 3:
        out[2] = slot[slotId].sketch_bytes[1];  // fallthrough
      case 2:
        out[1] = slot[slotId].sketch_bytes[2];  // fallthrough
      case 1:
        out[0] = slot[slotId].sketch_bytes[3];  // fallthrough
      case 0:
        break;
      default:
        __builtin_unreachable();  // mmmm, dangerous
    };
  }
  // -------------------------------------------------------------------------------------
  static unsigned spaceNeeded(unsigned keyLength, unsigned prefixLength);
  static int cmpKeys(u8* a, u8* b, unsigned aLength, unsigned bLength);
  static SketchType head(u8*& key, unsigned& keyLength);
  void makeHint();
  // -------------------------------------------------------------------------------------
  bool sanityCheck(u8* key, unsigned keyLength);
  // -------------------------------------------------------------------------------------
  template <bool equalityOnly = false>
  s32 lowerBound(u8* key, unsigned keyLength)
  {
    // for (unsigned i=1; i<count; i++)
    // assert(slot[i-1].sketch <= slot[i].sketch);

    if (equalityOnly) {
      if ((keyLength < prefix_length) || (bcmp(key, getLowerFenceKey(), prefix_length) != 0))
        return -1;
    } else {
      int prefixCmp = cmpKeys(key, getLowerFenceKey(), min<unsigned>(keyLength, prefix_length), prefix_length);
      if (prefixCmp < 0)
        return 0;
      else if (prefixCmp > 0)
        return count;
    }
    key += prefix_length;
    keyLength -= prefix_length;

    unsigned lower = 0;
    unsigned upper = count;

    unsigned oldKeyLength = keyLength;
    SketchType keyHead = head(key, keyLength);

    if (count > hint_count * 2) {
      unsigned dist = count / (hint_count + 1);
      unsigned pos;
      for (pos = 0; pos < hint_count; pos++)
        if (hint[pos] >= keyHead)
          break;
      lower = pos * dist;
      unsigned pos2;
      for (pos2 = pos; pos2 < hint_count; pos2++)
        if (hint[pos2] != keyHead)
          break;
      if (pos2 < hint_count)
        upper = (pos2 + 1) * dist;
      // cout << is_leaf << " " << count << " " << lower << " " << upper << " "
      // << dist << endl;
    }

    while (lower < upper) {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (keyHead < slot[mid].sketch) {
        upper = mid;
      } else if (keyHead > slot[mid].sketch) {
        lower = mid + 1;
      } else if (slot[mid].rest_len == 0) {
        if (oldKeyLength < slot[mid].head_len) {
          upper = mid;
        } else if (oldKeyLength > slot[mid].head_len) {
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
  // -------------------------------------------------------------------------------------
  void updateHint(unsigned slotId);
  // -------------------------------------------------------------------------------------
  bool insert(u8* key, unsigned keyLength, ValueType value, u8* payload = nullptr);
  bool canInsert(u8* key, unsigned keyLength, ValueType value, u8* payload = nullptr);
  bool update(u8* key, unsigned keyLength, u16 payload_length, u8* payload);
  // -------------------------------------------------------------------------------------
  void compactify();
  // -------------------------------------------------------------------------------------
  // merge right node into this node
  u32 mergeSpaceUpperBound(ExclusivePageGuard<BTreeNode>& right);
  bool merge(unsigned slotId, ExclusivePageGuard<BTreeNode>& parent, ExclusivePageGuard<BTreeNode>& right);
  // store key/value pair at slotId
  void storeKeyValue(u16 slotId, u8* key, unsigned keyLength, ValueType value, u8* payload = nullptr);
  void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, unsigned count);
  void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot);
  void insertFence(FenceKey& fk, u8* key, unsigned keyLength);
  void setFences(u8* lowerKey, unsigned lowerLen, u8* upperKey, unsigned upperLen);
  void split(ExclusivePageGuard<BTreeNode>& parent, ExclusivePageGuard<BTreeNode>& new_node, unsigned sepSlot, u8* sepKey, unsigned sepLength);
  unsigned commonPrefix(unsigned aPos, unsigned bPos);
  SeparatorInfo findSep();
  void getSep(u8* sepKeyOut, SeparatorInfo info);
  Swip<BTreeNode>& lookupInner(u8* key, unsigned keyLength);
  // -------------------------------------------------------------------------------------
  // Not synchronized or todo section
  bool removeSlot(u16 slotId);
  bool remove(u8* key, unsigned keyLength);
};
// -------------------------------------------------------------------------------------
static_assert(sizeof(BTreeNode) == EFFECTIVE_PAGE_SIZE, "page size problem");
// -------------------------------------------------------------------------------------
}  // namespace vs
}  // namespace btree
}  // namespace leanstore
