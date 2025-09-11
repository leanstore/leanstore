#pragma once
#include "Swip.hpp"
#include "Units.hpp"
#include "leanstore/sync-primitives/Latch.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cstring>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
const u64 PAGE_SIZE = 4 * 1024;
// -------------------------------------------------------------------------------------
struct BufferFrame {
   enum class STATE : u8 { FREE = 0, HOT = 1, COOL = 2, LOADED = 3 };
   struct Header {
      WORKERID last_writer_worker_id = std::numeric_limits<u8>::max();  // for RFA
      LID last_written_plsn = 0;
      STATE state = STATE::FREE;  // INIT:
      std::atomic<bool> is_being_written_back = false;
      bool keep_in_memory = false;
      PID pid = 9999;         // INIT:
      HybridLatch latch = 0;  // INIT: // ATTENTION: NEVER DECREMENT
      // -------------------------------------------------------------------------------------
      BufferFrame* next_free_bf = nullptr;
      // -------------------------------------------------------------------------------------
      // Contention Split data structure
      struct ContentionTracker {
         u32 restarts_counter = 0;
         u32 access_counter = 0;
         s32 last_modified_pos = -1;
         void reset()
         {
            restarts_counter = 0;
            access_counter = 0;
            last_modified_pos = -1;
         }
      };
      ContentionTracker contention_tracker;
      // -------------------------------------------------------------------------------------
      struct OptimisticParentPointer {
         BufferFrame* parent_bf = nullptr;
         PID parent_pid;
         LID parent_plsn = 0;
         BufferFrame** swip_ptr = nullptr;
         s64 pos_in_parent = -1;
         void update(BufferFrame* new_parent_bf, PID new_parent_pid, LID new_parent_gsn, BufferFrame** new_swip_ptr, s64 new_pos_in_parent)
         {
            if (parent_bf != new_parent_bf || parent_pid != new_parent_pid || parent_plsn != new_parent_gsn || swip_ptr != new_swip_ptr ||
                pos_in_parent != new_pos_in_parent) {
               parent_bf = new_parent_bf;
               parent_pid = new_parent_pid;
               parent_plsn = new_parent_gsn;
               swip_ptr = new_swip_ptr;
               pos_in_parent = new_pos_in_parent;
            }
         }
      };
      OptimisticParentPointer optimistic_parent_pointer;
      // -------------------------------------------------------------------------------------
      u64 crc = 0;
   };
   struct alignas(512) Page {
      LID PLSN = 0;
      LID GSN = 0;
      DTID dt_id = 9999;                                                                               // INIT: datastructure id
      u64 magic_debugging_number;                                                                      // ATTENTION
      u8 dt[PAGE_SIZE - sizeof(PLSN) - sizeof(GSN) - sizeof(dt_id) - sizeof(magic_debugging_number)];  // Datastruture BE CAREFUL HERE !!!!!
      // -------------------------------------------------------------------------------------
      operator u8*() { return reinterpret_cast<u8*>(this); }
      // -------------------------------------------------------------------------------------
   };
   // -------------------------------------------------------------------------------------
   struct Header header;
   // -------------------------------------------------------------------------------------
   struct Page page;  // The persisted part
   // -------------------------------------------------------------------------------------
   bool operator==(const BufferFrame& other) { return this == &other; }
   // -------------------------------------------------------------------------------------
   inline bool isDirty() const { return 
      page.PLSN != 0 && // marked as dirty
      page.PLSN != header.last_written_plsn // and not written
   ; }
   inline bool isFree() const { return header.state == STATE::FREE; }
   // -------------------------------------------------------------------------------------
   // Pre: bf is exclusively locked
   void reset()
   {
      header.crc = 0;
      // -------------------------------------------------------------------------------------
      assert(!header.is_being_written_back);
      header.latch.assertExclusivelyLatched();
      header.last_writer_worker_id = std::numeric_limits<u8>::max();
      header.last_written_plsn = 0;
      header.state = STATE::FREE;  // INIT:
      header.is_being_written_back.store(false, std::memory_order_release);
      header.pid = 9999;
      header.next_free_bf = nullptr;
      header.contention_tracker.reset();
      header.keep_in_memory = false;
      // std::memset(reinterpret_cast<u8*>(&page), 0, PAGE_SIZE);
   }
   // -------------------------------------------------------------------------------------
   BufferFrame() { header.latch->store(0ul); }
};
// -------------------------------------------------------------------------------------
static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(BufferFrame::Page::dt);
// -------------------------------------------------------------------------------------
static_assert(sizeof(BufferFrame::Page) == PAGE_SIZE, "");
// -------------------------------------------------------------------------------------
static_assert((sizeof(BufferFrame) - sizeof(BufferFrame::Page)) == 512, "");
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
