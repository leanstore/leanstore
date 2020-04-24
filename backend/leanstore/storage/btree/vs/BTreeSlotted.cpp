#include "BTreeSlotted.hpp"

#include "leanstore/sync-primitives/PageGuard.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace btree
{
namespace vs
{
// -------------------------------------------------------------------------------------
// calculate space needed for keys in inner nodes.
u16 BTreeNode::spaceNeeded(u16 keyLength, u16 prefixLength)
{
  //assert(keyLength >= prefixLength);
  u16 restLen = keyLength - prefixLength;
  if (restLen <= 4)
    return sizeof(Slot) + sizeof(ValueType);
  restLen -= sizeof(SketchType);
  return sizeof(Slot) + restLen + sizeof(ValueType) + ((restLen > largeLimit) ? sizeof(u16) : 0);
}
// -------------------------------------------------------------------------------------
int BTreeNode::cmpKeys(u8* a, u8* b, u16 aLength, u16 bLength)
{
  int c = memcmp(a, b, min(aLength, bLength));
  if (c)
    return c;
  return (aLength - bLength);
}
// -------------------------------------------------------------------------------------
SketchType BTreeNode::head(u8*& key, u16& keyLength)
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
// -------------------------------------------------------------------------------------
void BTreeNode::makeHint()
{
  u16 dist = count / (hint_count + 1);
  for (u16 i = 0; i < hint_count; i++)
    hint[i] = slot[dist * (i + 1)].sketch;
}
// -------------------------------------------------------------------------------------
void BTreeNode::updateHint(u16 slotId)
{
  u16 dist = count / (hint_count + 1);
  u16 begin = 0;
  if ((count > hint_count * 2 + 1) && (((count - 1) / (hint_count + 1)) == dist) && ((slotId / dist) > 1))
    begin = (slotId / dist) - 1;
  for (u16 i = begin; i < hint_count; i++)
    hint[i] = slot[dist * (i + 1)].sketch;
  for (u16 i = 0; i < hint_count; i++)
    assert(hint[i] == slot[dist * (i + 1)].sketch);
}
// -------------------------------------------------------------------------------------
u16 BTreeNode::spaceNeeded(u16 key_length, ValueType value)
{
  const u16 space_needed = (is_leaf) ? value.raw() + spaceNeeded(key_length, prefix_length) : spaceNeeded(key_length, prefix_length);
  return space_needed;
}
// -------------------------------------------------------------------------------------
bool BTreeNode::canInsert(u16 keyLength, ValueType value)
{
  const u16 space_needed = (is_leaf) ? value.raw() + spaceNeeded(keyLength, prefix_length) : spaceNeeded(keyLength, prefix_length);
  if (!hasEnoughSpaceFor(space_needed))
    return false;  // no space, insert fails
  else
    return true;
}
// -------------------------------------------------------------------------------------
bool BTreeNode::prepareInsert(u8* key, u16 keyLength, ValueType value)
{
  s32 sanity_check_result = sanityCheck(key, keyLength);
  static_cast<void>(sanity_check_result);
  assert(sanity_check_result == 0);
  // -------------------------------------------------------------------------------------
  const u16 space_needed = (is_leaf) ? value.raw() + spaceNeeded(keyLength, prefix_length) : spaceNeeded(keyLength, prefix_length);
  if (!requestSpaceFor(space_needed))
    return false;  // no space, insert fails
  else
    return true;
}
// -------------------------------------------------------------------------------------
void BTreeNode::insert(u8* key, u16 keyLength, ValueType value, u8* payload)
{
  DEBUG_BLOCK()
  {
    assert(prepareInsert(key, keyLength, value));
    s32 exact_pos = lowerBound<true>(key, keyLength);
    static_cast<void>(exact_pos);
    assert(exact_pos == -1);  // assert for duplicates
  }
  s32 slotId = lowerBound<false>(key, keyLength);
  memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
  storeKeyValue(slotId, key, keyLength, value, payload);
  count++;
  updateHint(slotId);
  DEBUG_BLOCK()
  {
    s32 exact_pos = lowerBound<true>(key, keyLength);
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
  u16 leftGrow = (prefix_length - tmp.prefix_length) * count;
  u16 rightGrow = (right->prefix_length - tmp.prefix_length) * right->count;
  u16 spaceUpperBound = space_used + right->space_used + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
  return spaceUpperBound;
}
// -------------------------------------------------------------------------------------
u32 BTreeNode::spaceUsedBySlot(u16 s_i)
{
  u32 space = 0;
  if (slot[s_i].rest_len) {
    space += (isLarge(s_i) ? (getRestLenLarge(s_i) + sizeof(u16)) : getRestLen(s_i));
  }
  space += sizeof(ValueType) + sizeof(BTreeNode::Slot) + (is_leaf ? getPayloadLength(s_i) : 0);
  return space;
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
  } else {
    assert(!right->is_leaf);
    assert(parent->isInner());
    BTreeNode tmp(is_leaf);
    tmp.setFences(getLowerFenceKey(), lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
    u16 leftGrow = (prefix_length - tmp.prefix_length) * count;
    u16 rightGrow = (right->prefix_length - tmp.prefix_length) * right->count;
    u16 extraKeyLength = parent->getFullKeyLength(slotId);
    u16 spaceUpperBound = space_used + right->space_used + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow +
                               spaceNeeded(extraKeyLength, tmp.prefix_length);
    if (spaceUpperBound > EFFECTIVE_PAGE_SIZE)
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
    memcpy(reinterpret_cast<u8*>(right.ptr()), &tmp, sizeof(BTreeNode));
    return true;
  }
}
// -------------------------------------------------------------------------------------
void BTreeNode::storeKeyValue(u16 slotId, u8* key, u16 keyLength, ValueType value, u8* payload)
{
  // Head
  key += prefix_length;
  keyLength -= prefix_length;
  slot[slotId].head_len = (keyLength >= sizeof(SketchType)) ? sizeof(SketchType) : keyLength;
  slot[slotId].sketch = head(key, keyLength);
  // Value
  u16 space = keyLength + sizeof(ValueType) + ((keyLength > largeLimit) ? sizeof(u16) : 0) + ((is_leaf) ? value.raw() : 0);
  data_offset -= space;
  space_used += space;
  slot[slotId].offset = data_offset;
  if (is_leaf) {
    getPayloadLength(slotId) = static_cast<u16>(value.raw());
  } else {
    getValue(slotId) = value;
  }
  // Rest
  if (keyLength > largeLimit) {  // large string
    setLarge(slotId);
    getRestLenLarge(slotId) = keyLength;
    memcpy(getRestLarge(slotId), key, keyLength);
    // store payload
    if (is_leaf) {  // AAA
      assert(payload != nullptr);
      memcpy(getPayloadLarge(slotId), payload, getPayloadLength(slotId));
    }
  } else {  // normal string
    slot[slotId].rest_len = keyLength;
    memcpy(getRest(slotId), key, keyLength);
    // store payload
    if (is_leaf) {  // AAA
      assert(payload != nullptr);
      memcpy(getPayload(slotId), payload, getPayloadLength(slotId));
    }
  }
  assert(ptr() + data_offset >= reinterpret_cast<u8*>(slot + count));
}
// -------------------------------------------------------------------------------------
// ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
void BTreeNode::copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, u16 count)
{
  if (is_leaf && count == u16(this->count - 1) && dst->count == 0) {
    // raise(SIGTRAP);
  }
  if (prefix_length == dst->prefix_length) {
    // Fast path
    memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
    DEBUG_BLOCK()
    {
      u32 total_space_used = upper_fence.length + lower_fence.length;
      for (u16 i = 0; i < this->count; i++) {
        total_space_used += sizeof(ValueType) + (isLarge(i) ? (getRestLenLarge(i) + sizeof(u16)) : getRestLen(i));
        total_space_used += (is_leaf) ? getPayloadLength(i) : 0;
      }
      assert(total_space_used == this->space_used);
    }
    for (u16 i = 0; i < count; i++) {
      u32 kv_size = sizeof(ValueType) + (isLarge(srcSlot + i) ? (getRestLenLarge(srcSlot + i) + sizeof(u16)) : getRestLen(srcSlot + i));
      if (dst->is_leaf) {
        assert(is_leaf);
        kv_size += getPayloadLength(srcSlot + i);  // AAA: Payload size
      }
      dst->data_offset -= kv_size;
      dst->space_used += kv_size;
      dst->slot[dstSlot + i].offset = dst->data_offset;
      assert((dst->ptr() + dst->data_offset) >= reinterpret_cast<u8*>(dst->slot + dstSlot + count));
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
  u16 fullLength = getFullKeyLength(srcSlot);
  u8 key[fullLength];
  copyFullKey(srcSlot, key, fullLength);
  u8* payload = (is_leaf) ? ((isLarge(srcSlot) ? getPayloadLarge(srcSlot) : getPayload(srcSlot))) : nullptr;
  dst->storeKeyValue(dstSlot, key, fullLength, getValue(srcSlot), payload);
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
u16 BTreeNode::commonPrefix(u16 aPos, u16 bPos)
{
  // TODO: the folowing two checks work only in single threaded
  //   assert(aPos < count);
  //   assert(bPos < count);
  if ((slot[aPos].sketch == slot[bPos].sketch) && (slot[aPos].head_len == slot[bPos].head_len)) {
    u16 aLen, bLen;
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
    u16 i;
    for (i = 0; i < min(aLen, bLen); i++)
      if (a[i] != b[i])
        break;
    return i + slot[aPos].head_len;
  }
  u16 limit = min(slot[aPos].head_len, slot[bPos].head_len);
  u16 i;
  for (i = 0; i < limit; i++)
    if (slot[aPos].sketch_bytes[3 - i] != slot[bPos].sketch_bytes[3 - i])
      return i;
  return i;
}
// -------------------------------------------------------------------------------------
BTreeNode::SeparatorInfo BTreeNode::findSep()
{
  if (isInner())
    return SeparatorInfo{getFullKeyLength(count / 2), static_cast<u16>(count / 2), false};

  u16 lower = count / 2 - count / 16;
  u16 upper = count / 2 + count / 16;
  //   assert(upper < count); TODO
  u16 maxPos = count / 2;
  int maxPrefix = commonPrefix(maxPos, 0);
  for (u16 i = lower; i < upper; i++) {
    int prefix = commonPrefix(i, 0);
    if (prefix > maxPrefix) {
      maxPrefix = prefix;
      maxPos = i;
    }
  }
  u16 common = commonPrefix(maxPos, maxPos + 1);
  if ((common > sizeof(SketchType)) && (getFullKeyLength(maxPos) - prefix_length > common) &&
      (getFullKeyLength(maxPos + 1) - prefix_length > common + 2)) {
    return SeparatorInfo{static_cast<u16>(prefix_length + common + 1), maxPos, true};
  }
  return SeparatorInfo{getFullKeyLength(maxPos), maxPos, false};
}
// -------------------------------------------------------------------------------------
void BTreeNode::getSep(u8* sepKeyOut, BTreeNodeHeader::SeparatorInfo info)
{
  copyFullKey(info.slot, sepKeyOut, info.length);
  if (info.trunc) {  // TODO: ??
    u8* k = isLarge(info.slot + 1) ? getRestLarge(info.slot + 1) : getRest(info.slot + 1);
    sepKeyOut[info.length - 1] = k[info.length - prefix_length - sizeof(SketchType) - 1];
  }
}
// -------------------------------------------------------------------------------------
s32 BTreeNode::sanityCheck(u8* key, u16 keyLength)
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
Swip<BTreeNode>& BTreeNode::lookupInner(u8* key, u16 keyLength)
{
  s32 pos = lowerBound<false>(key, keyLength);
  if (pos == count)
    return upper;
  return getValue(pos);
}
// -------------------------------------------------------------------------------------
void BTreeNode::split(ExclusivePageGuard<BTreeNode>& parent,
                      ExclusivePageGuard<BTreeNode>& nodeLeft,
                      u16 sepSlot,
                      u8* sepKey,
                      u16 sepLength)
{
  // PRE: current, parent and nodeLeft are x locked
  // assert(sepSlot > 0); TODO: really ?
  assert(sepSlot < (EFFECTIVE_PAGE_SIZE / sizeof(ValueType)));
  // -------------------------------------------------------------------------------------
  nodeLeft->setFences(getLowerFenceKey(), lower_fence.length, sepKey, sepLength);
  BTreeNode tmp(is_leaf);
  BTreeNode* nodeRight = &tmp;
  nodeRight->setFences(sepKey, sepLength, getUpperFenceKey(), upper_fence.length);
  assert(parent->prepareInsert(sepKey, sepLength, nodeLeft.swip()));
  parent->insert(sepKey, sepLength, nodeLeft.swip());
  if (is_leaf) {
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
  memcpy(reinterpret_cast<char*>(this), nodeRight, sizeof(BTreeNode));
}
// -------------------------------------------------------------------------------------
bool BTreeNode::removeSlot(u16 slotId)
{
  if (slot[slotId].rest_len) {
    space_used -= (isLarge(slotId) ? (getRestLenLarge(slotId) + sizeof(u16)) : getRestLen(slotId));
  }
  space_used -= sizeof(ValueType) + ((is_leaf) ? getPayloadLength(slotId) : 0);
  memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
  count--;
  makeHint();
  return true;
}
// -------------------------------------------------------------------------------------
bool BTreeNode::remove(u8* key, u16 keyLength)
{
  int slotId = lowerBound<true>(key, keyLength);
  if (slotId == -1)
    return false;  // key not found
  return removeSlot(slotId);
}
// -------------------------------------------------------------------------------------
}  // namespace vs
}  // namespace btree
}  // namespace leanstore
