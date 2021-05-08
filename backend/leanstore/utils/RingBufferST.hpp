#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <memory>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
// Variable-length singel-threaded ring buffer
class RingBufferST
{
  private:
   struct Entry {
      u8 is_cr : 1;
      u64 length : 63;
      u8 payload[];
   };
   static_assert(sizeof(Entry) == sizeof(u64), "");
   // -------------------------------------------------------------------------------------
   const u64 buffer_size;
   std::unique_ptr<u8[]> buffer;
   u64 write_cursor = 0, read_cursor = 0;
   // -------------------------------------------------------------------------------------
   u64 continguousFreeSpace() { return (read_cursor > write_cursor) ? read_cursor - write_cursor : buffer_size - write_cursor; }
   u64 freeSpace()
   {
      // A , B , C : a - b + c % c
      if (write_cursor == read_cursor) {
         return buffer_size;
      } else if (read_cursor < write_cursor) {
         return read_cursor + (buffer_size - write_cursor);
      } else {
         return read_cursor - write_cursor;
      }
   }
   void carriageReturn()
   {
      reinterpret_cast<Entry*>(buffer.get() + write_cursor)->is_cr = 1;
      write_cursor = 0;
   }
   // -------------------------------------------------------------------------------------
  public:
   RingBufferST(u64 size) : buffer_size(size) { buffer = std::make_unique<u8[]>(buffer_size); }
   bool canInsert(u64 payload_size)
   {
      const u64 total_size = payload_size + sizeof(Entry);
      const u64 contiguous_free_space = continguousFreeSpace();
      if (contiguous_free_space >= total_size) {
         return true;
      } else {
         const u64 free_space = freeSpace();
         const u64 free_space_beginning = free_space - contiguous_free_space;
         return free_space_beginning >= total_size;
      }
   }
   u8* pushBack(u64 payload_length)
   {
      ensure(canInsert(payload_length));
      const u64 total_size = payload_length + sizeof(Entry);
      const u64 contiguous_free_space = continguousFreeSpace();
      if (contiguous_free_space < total_size) {
         carriageReturn();
      }
      // -------------------------------------------------------------------------------------
      auto& entry = *reinterpret_cast<Entry*>(buffer.get() + write_cursor);
      write_cursor += total_size;
      entry.is_cr = 0;
      entry.length = payload_length;
      return entry.payload;
   }
   void iterateUntilTail(u8* start_payload_ptr, std::function<void(u8* entry)> cb)
   {
      cb(start_payload_ptr);
      start_payload_ptr -= sizeof(Entry);
      u64 cursor = start_payload_ptr - buffer.get();
      while (cursor != write_cursor) {
         auto entry = reinterpret_cast<Entry*>(buffer.get() + cursor);
         if (entry->is_cr) {
            cursor = 0;
            entry = reinterpret_cast<Entry*>(buffer.get());
         } else {
            cb(entry->payload);
            cursor += entry->length + sizeof(Entry);
         }
      }
   }
   bool empty() { return write_cursor == read_cursor; }
   u8* front()
   {
      auto entry = reinterpret_cast<Entry*>(buffer.get() + read_cursor);
      if (entry->is_cr) {
         read_cursor = 0;
         entry = reinterpret_cast<Entry*>(buffer.get());
      }
      return entry->payload;
   }
   void popFront() { read_cursor += reinterpret_cast<Entry*>(buffer.get() + read_cursor)->length + sizeof(Entry); }
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
