#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <immintrin.h>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>
#include <string>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
struct BTreeNode;
using SwipType = Swip<BTreeNode>;
using HeadType = u32;
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
static inline u8 swap(u8 x)
{
   return x;
}
// -------------------------------------------------------------------------------------
struct BTreeNodeHeader {
   static const u16 underFullSize = EFFECTIVE_PAGE_SIZE * 0.6;
   static const u16 K_WAY_MERGE_THRESHOLD = EFFECTIVE_PAGE_SIZE * 0.45;

   struct SeparatorInfo {
      u16 length;
      u16 slot;
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
   u16 space_used = 0;  // does not include the header, but includes fences !!!!!
   u16 data_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);
   u16 prefix_length = 0;

   static const u16 hint_count = 16;
   u32 hint[hint_count];
   // -------------------------------------------------------------------------------------
   // Needed for GC
   bool has_garbage = false;
   // -------------------------------------------------------------------------------------
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
   struct __attribute__((packed)) Slot {
      // Layout:  key wihtout prefix | Payload
      u16 offset;
      u16 key_len;
      u16 payload_len;
      union {
         HeadType head;
         u8 head_bytes[4];
      };
   };
   // Just to make sizeof(BTreeNode) == EFFECTIVE_PAGE_SIZE
   static constexpr u64 max_theorical_slots_capacity = (EFFECTIVE_PAGE_SIZE - sizeof(BTreeNodeHeader)) / (sizeof(Slot));
   static constexpr u64 left_space_to_waste = (EFFECTIVE_PAGE_SIZE - sizeof(BTreeNodeHeader)) % (sizeof(Slot));
   Slot slot[max_theorical_slots_capacity];
   u8 padding[left_space_to_waste];

   BTreeNode(bool is_leaf) : BTreeNodeHeader(is_leaf) {}

   u16 freeSpace() { return data_offset - (reinterpret_cast<u8*>(slot + count) - ptr()); }
   u16 freeSpaceAfterCompaction() { return EFFECTIVE_PAGE_SIZE - (reinterpret_cast<u8*>(slot + count) - ptr()) - space_used; }
   // -------------------------------------------------------------------------------------
   double fillFactorAfterCompaction() { return (1 - (freeSpaceAfterCompaction() * 1.0 / EFFECTIVE_PAGE_SIZE)); }
   // -------------------------------------------------------------------------------------
   bool hasEnoughSpaceFor(u32 space_needed) { return (space_needed <= freeSpace() || space_needed <= freeSpaceAfterCompaction()); }
   // ATTENTION: this method has side effects !
   bool requestSpaceFor(u16 space_needed)
   {
      if (space_needed <= freeSpace())
         return true;
      if (space_needed <= freeSpaceAfterCompaction()) {
         compactify();
         return true;
      }
      return false;
   }
   // -------------------------------------------------------------------------------------
   inline u8* getKey(u16 slotId) { return ptr() + slot[slotId].offset; }
   inline u16 getKeyLen(u16 slotId) { return slot[slotId].key_len; }
   inline u16 getFullKeyLen(u16 slotId) { return prefix_length + getKeyLen(slotId); }
   inline u16 getPayloadLength(u16 slotId) { return slot[slotId].payload_len; }
   inline u8* getPayload(u16 slotId) { return ptr() + slot[slotId].offset + slot[slotId].key_len; }
   inline SwipType& getChild(u16 slotId) { return *reinterpret_cast<SwipType*>(getPayload(slotId)); }
   inline u16 getKVConsumedSpace(u16 slot_id) { return sizeof(Slot) + getKeyLen(slot_id) + getPayloadLength(slot_id); }
   // -------------------------------------------------------------------------------------
   // Attention: the caller has to hold a copy of the existing payload
   inline void shortenPayload(u16 slotId, u16 len)
   {
      assert(len <= slot[slotId].payload_len);
      const u16 freed_space = slot[slotId].payload_len - len;
      space_used -= freed_space;
      slot[slotId].payload_len = len;
   }
   inline bool canExtendPayload(u16 slot_id, u16 new_length)
   {
      assert(new_length > getPayloadLength(slot_id));
      const u16 extra_space_needed = new_length - getPayloadLength(slot_id);
      return freeSpaceAfterCompaction() >= extra_space_needed;
   }
   void extendPayload(u16 slot_id, u16 new_payload_length)
   {
      // Move key | payload to a new location
      assert(canExtendPayload(slot_id, new_payload_length));
      const u16 extra_space_needed = new_payload_length - getPayloadLength(slot_id);
      requestSpaceFor(extra_space_needed);
      // -------------------------------------------------------------------------------------
      const u16 key_length = getKeyLen(slot_id);
      const u16 old_total_length = key_length + getPayloadLength(slot_id);
      const u16 new_total_length = key_length + new_payload_length;
      u8 key[key_length];
      std::memcpy(key, getKey(slot_id), key_length);
      space_used -= old_total_length;
      if (data_offset == slot[slot_id].offset && 0) {
         data_offset += old_total_length;
      }
      slot[slot_id].payload_len = 0;
      slot[slot_id].key_len = 0;
      if (freeSpace() < new_total_length) {
         compactify();
      }
      assert(freeSpace() >= new_total_length);
      space_used += new_total_length;
      data_offset -= new_total_length;
      slot[slot_id].offset = data_offset;
      slot[slot_id].key_len = key_length;
      slot[slot_id].payload_len = new_payload_length;
      std::memcpy(getKey(slot_id), key, key_length);
   }
   // -------------------------------------------------------------------------------------
   inline u8* getPrefix() { return getLowerFenceKey(); }
   inline void copyPrefix(u8* out) { memcpy(out, getLowerFenceKey(), prefix_length); }
   inline void copyKeyWithoutPrefix(u16 slotId, u8* out_after_prefix) { memcpy(out_after_prefix, getKey(slotId), getKeyLen(slotId)); }
   inline void copyFullKey(u16 slotId, u8* out)
   {
      memcpy(out, getPrefix(), prefix_length);
      memcpy(out + prefix_length, getKey(slotId), getKeyLen(slotId));
   }
   // -------------------------------------------------------------------------------------
   static inline s32 cmpKeys(const u8* a, const u8* b, u16 a_length, u16 b_length)
   {
      u16 length = min(a_length, b_length);
      if (length < 4) {
         while (length-- > 0) {
            if (*a++ != *b++)
               return a[-1] < b[-1] ? -1 : 1;
         }
         return (a_length - b_length);
      } else {
         int c = memcmp(a, b, length);
         if (c != 0)
            return c;
         return (a_length - b_length);
      }
   }
   static inline HeadType head(const u8* key, u16& key_length)
   {
      switch (key_length) {
         case 0:
            return 0;
         case 1:
            return static_cast<u32>(key[0]) << 24;
         case 2:
            return static_cast<u32>(__builtin_bswap16(*reinterpret_cast<const u16*>(key))) << 16;
         case 3:
            return (static_cast<u32>(__builtin_bswap16(*reinterpret_cast<const u16*>(key))) << 16) | (static_cast<u32>(key[2]) << 8);
         default:
            return __builtin_bswap32(*reinterpret_cast<const u32*>(key));
      }
   }
   void makeHint();
   // -------------------------------------------------------------------------------------
   s32 compareKeyWithBoundaries(const u8* key, u16 key_length);
   // -------------------------------------------------------------------------------------
   void searchHint(HeadType key_head, u16& lower_out, u16& upper_out)
   {
      if (count > hint_count * 2) {
         if (FLAGS_btree_hints == 2) {
#ifdef __AVX512F__
            const u16 dist = count / (hint_count + 1);
            u16 pos, pos2;
            __m512i key_head_reg = _mm512_set1_epi32(key_head);
            __m512i chunk = _mm512_loadu_si512(hint);
            __mmask16 compareMask = _mm512_cmpge_epu32_mask(chunk, key_head_reg);
            if (compareMask == 0)
               return;
            pos = __builtin_ctz(compareMask);
            lower_out = pos * dist;
            // -------------------------------------------------------------------------------------
            for (pos2 = pos; pos2 < hint_count; pos2++) {
               if (hint[pos2] != key_head) {
                  break;
               }
            }
            if (pos2 < hint_count) {
               upper_out = (pos2 + 1) * dist;
            }
#else
            throw;
#endif
         } else if (FLAGS_btree_hints == 1) {
            const u16 dist = count / (hint_count + 1);
            u16 pos, pos2;
            // -------------------------------------------------------------------------------------
            for (pos = 0; pos < hint_count; pos++) {
               if (hint[pos] >= key_head) {
                  break;
               }
            }
            for (pos2 = pos; pos2 < hint_count; pos2++) {
               if (hint[pos2] != key_head) {
                  break;
               }
            }
            // -------------------------------------------------------------------------------------
            lower_out = pos * dist;
            if (pos2 < hint_count) {
               upper_out = (pos2 + 1) * dist;
            }
            // -------------------------------------------------------------------------------------
            WorkerCounters::myCounters().dt_researchy[0][0]++;
            WorkerCounters::myCounters().dt_researchy[0][1] += pos > 0 || pos2 < hint_count;
         } else {
         }
      }
   }
   // -------------------------------------------------------------------------------------
   template <bool equality_only = false>
   s16 linearSearchWithBias(const u8* key, u16 key_length, u16 start_pos, bool higher = true)
   {
      throw;
      // EXP
      if (key_length < prefix_length || (bcmp(key, getLowerFenceKey(), prefix_length) != 0)) {
         return -1;
      }
      assert((key_length >= prefix_length) && (bcmp(key, getLowerFenceKey(), prefix_length) == 0));
      // the compared key has the same prefix
      key += prefix_length;
      key_length -= prefix_length;

      if (higher) {
         // assert(cmpKeys(key, getKey(start_pos), key_length, getKeyLen(start_pos)) >= 0);
         s32 cur = start_pos + 1;
         for (; cur < count; cur++) {
            int cmp = cmpKeys(key, getKey(cur), key_length, getKeyLen(cur));
            if (cmp == 0) {
               return cur;
            } else {
               break;
            }
         }
         if (equality_only) {
            return -1;
         } else {
            return cur;
         }
      } else {
         s32 cur = start_pos - 1;
         for (; cur >= 0; cur--) {
            int cmp = cmpKeys(key, getKey(cur), key_length, getKeyLen(cur));
            if (cmp == 0) {
               return cur;
            } else {
               break;
            }
         }
         if (equality_only) {
            return -1;
         } else {
            return cur;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   // Returns the position where the key[pos] (if exists) >= key (not less than the given key)
   // Asc: (2) (2) (1) -> (2) (2) (1) (0) -> (2) (2) (1) (0) (0) -> ...  -> (2) (2) (2)
   template <bool equalityOnly = false>
   s16 lowerBound(const u8* key, u16 keyLength, bool* is_equal = nullptr)
   {
      if (is_equal != nullptr && is_leaf) {
         *is_equal = false;
      }
      if (equalityOnly) {
         if ((keyLength < prefix_length) || (bcmp(key, getLowerFenceKey(), prefix_length) != 0))
            return -1;
      } else {
         int prefixCmp = cmpKeys(key, getLowerFenceKey(), min<u16>(keyLength, prefix_length), prefix_length);
         if (prefixCmp < 0)
            return 0;
         else if (prefixCmp > 0)
            return count;
      }
      // the compared key has the same prefix
      key += prefix_length;
      keyLength -= prefix_length;

      u16 lower = 0;
      u16 upper = count;
      HeadType keyHead = head(key, keyLength);
      searchHint(keyHead, lower, upper);
      while (lower < upper) {
         u16 mid = ((upper - lower) / 2) + lower;
         if (FLAGS_btree_heads) {
            if (keyHead < slot[mid].head) {
               upper = mid;
            } else if (keyHead > slot[mid].head) {
               lower = mid + 1;
            } else if (slot[mid].key_len <= 4) {
               // head is equal, we don't have to check the rest of the key
               if (keyLength < slot[mid].key_len) {
                  upper = mid;
               } else if (keyLength > slot[mid].key_len) {
                  lower = mid + 1;
               } else {
                  if (is_equal != nullptr && is_leaf) {
                     *is_equal = true;
                  }
                  return mid;  // It is even equal
               }
            } else {
               int cmp = cmpKeys(key, getKey(mid), keyLength, getKeyLen(mid));
               if (cmp < 0) {
                  upper = mid;
               } else if (cmp > 0) {
                  lower = mid + 1;
               } else {
                  if (is_equal != nullptr && is_leaf) {
                     *is_equal = true;
                  }
                  return mid;  // It is even equal
               }
            }
         } else {
            int cmp = cmpKeys(key, getKey(mid), keyLength, getKeyLen(mid));
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               if (is_equal != nullptr && is_leaf) {
                  *is_equal = true;
               }
               return mid;  // It is even equal
            }
         }
      }
      if (equalityOnly)
         return -1;
      return lower;
   }
   // -------------------------------------------------------------------------------------
   void updateHint(u16 slotId);
   // -------------------------------------------------------------------------------------
   s16 insertDoNotCopyPayload(const u8* key, u16 key_len, u16 payload_len, s32 pos = -1);
   s32 insert(const u8* key, u16 key_len, const u8* payload, u16 payload_len);
   static u16 spaceNeeded(u16 keyLength, u16 payload_len, u16 prefixLength);
   u16 spaceNeeded(u16 key_length, u16 payload_len);
   bool canInsert(u16 key_length, u16 payload_len);
   bool prepareInsert(u16 keyLength, u16 payload_len);
   // -------------------------------------------------------------------------------------
   void compactify();
   // -------------------------------------------------------------------------------------
   // merge right node into this node
   u32 mergeSpaceUpperBound(ExclusivePageGuard<BTreeNode>& right);
   u32 spaceUsedBySlot(u16 slot_id);
   // -------------------------------------------------------------------------------------
   bool merge(u16 slotId, ExclusivePageGuard<BTreeNode>& parent, ExclusivePageGuard<BTreeNode>& right);
   // store key/value pair at slotId
   void storeKeyValue(u16 slotId, const u8* key, u16 key_len, const u8* payload, u16 payload_len);
   // ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, u16 count);
   void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot);
   void insertFence(FenceKey& fk, u8* key, u16 keyLength);
   void setFences(u8* lowerKey, u16 lowerLen, u8* upperKey, u16 upperLen);
   void split(ExclusivePageGuard<BTreeNode>& parent, ExclusivePageGuard<BTreeNode>& new_node, u16 sepSlot, u8* sepKey, u16 sepLength);
   u16 commonPrefix(u16 aPos, u16 bPos);
   SeparatorInfo findSep();
   void getSep(u8* sepKeyOut, SeparatorInfo info);
   Swip<BTreeNode>& lookupInner(const u8* key, u16 keyLength);
   // -------------------------------------------------------------------------------------
   // Not synchronized or todo section
   bool removeSlot(u16 slotId);
   bool remove(const u8* key, const u16 keyLength);
   void reset();
};  // namespace btree
// -------------------------------------------------------------------------------------
static_assert(sizeof(BTreeNode) == EFFECTIVE_PAGE_SIZE, "BTreeNode must be equal to one page");
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
