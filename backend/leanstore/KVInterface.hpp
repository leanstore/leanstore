#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
enum class OP_RESULT : u8 { OK = 0, NOT_FOUND = 1, DUPLICATE = 2, ABORT_TX = 3, NOT_ENOUGH_SPACE = 4, OTHER = 5 };
struct UpdateSameSizeInPlaceDescriptor {
   u8 count = 0;
   struct Slot {
      u16 offset;
      u16 length;
      bool operator==(const Slot& other) const { return offset == other.offset && length == other.length; }
   };
   Slot slots[];
   u64 size() const { return sizeof(UpdateSameSizeInPlaceDescriptor) + (count * sizeof(UpdateSameSizeInPlaceDescriptor::Slot)); }
   u64 diffLength() const
   {
      u64 length = 0;
      for (u8 i = 0; i < count; i++) {
         length += slots[i].length;
      }
      return length;
   }
   u64 totalLength() const { return size() + diffLength(); }
   bool operator==(const UpdateSameSizeInPlaceDescriptor& other)
   {
      if (count != other.count)
         return false;
      for (u8 i = 0; i < count; i++) {
         if (slots[i].offset != other.slots[i].offset || slots[i].length != other.slots[i].length)
            return false;
      }
      return true;
   }
};
// -------------------------------------------------------------------------------------
// Interface
class KVInterface
{
  public:
   virtual OP_RESULT lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) = 0;
   virtual OP_RESULT updateSameSizeInPlace(u8* key,
                                           u16 key_length,
                                           std::function<void(u8* value, u16 value_size)>,
                                           UpdateSameSizeInPlaceDescriptor&) = 0;
   virtual OP_RESULT remove(u8* key, u16 key_length) = 0;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                             std::function<void()>) = 0;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                              std::function<void()>) = 0;
   // -------------------------------------------------------------------------------------
   virtual u64 countPages() = 0;
   virtual u64 countEntries() = 0;
   virtual u64 getHeight() = 0;
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT append(std::function<void(u8*)>, u16, std::function<void(u8*)>, u16, std::unique_ptr<u8[]>&) { return OP_RESULT::OTHER; }
   virtual OP_RESULT rangeRemove(u8*, u16, u8*, u16) { return OP_RESULT::OTHER; }
};
// -------------------------------------------------------------------------------------
using Slice = std::basic_string_view<u8>;
using StringU = std::basic_string<u8>;
struct MutableSlice {
   u8* ptr;
   u64 len;
   MutableSlice(u8* ptr, u64 len) : ptr(ptr), len(len) {}
   u64 length() { return len; }
   u8* data() { return ptr; }
};
}  // namespace leanstore
