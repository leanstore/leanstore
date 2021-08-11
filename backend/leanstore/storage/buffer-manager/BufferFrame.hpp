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
      u8 last_writer_worker_id = std::numeric_limits<u8>::max();  // for RFA
      u64 last_written_gsn = 0;
      STATE state = STATE::FREE;  // INIT:
      bool is_being_written_back = false;
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
      u64 debug;
   };
   struct alignas(512) Page {
      u64 GSN = 0;
      DTID dt_id = 9999;                                                                // INIT: datastructure id
      u64 magic_debugging_number;                                                       // ATTENTION
      u8 dt[PAGE_SIZE - sizeof(GSN) - sizeof(dt_id) - sizeof(magic_debugging_number)];  // Datastruture BE CAREFUL HERE !!!!!
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
   inline bool isDirty() const { return header.last_written_gsn != page.GSN; }
   // -------------------------------------------------------------------------------------
   // Pre: bf is exclusively locked
   void reset()
   {
      header.debug = header.pid;
      // -------------------------------------------------------------------------------------
      assert(!header.is_being_written_back);
      header.latch.assertExclusivelyLatched();
      header.last_writer_worker_id = std::numeric_limits<u8>::max();
      header.last_written_gsn = 0;
      header.state = STATE::FREE;  // INIT:
      header.is_being_written_back = false;
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
