#pragma once
#include "Units.hpp"
#include "Swip.hpp"
#include "leanstore/sync-primitives/OptimisticLock.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
const u64 PAGE_SIZE = 16 * 1024;
// -------------------------------------------------------------------------------------
struct BufferFrame {
   enum class State {
      FREE,
      HOT,
      COLD
   };
   struct Header {
      // TODO: for logging
      u64 lastWrittenLSN = 0;
      State state = State::FREE; // INIT:
      bool isWB = false;
      bool isCooledBecauseOfReading = false;
      PID pid = 9999; // INIT:
      OptimisticLock lock = 0;  // INIT:
      // -------------------------------------------------------------------------------------
      BufferFrame *next_free_bf = nullptr; //TODO
   };
   struct alignas(512) Page {
      u64 LSN = 0;
      u64 dt_id = 9999; //INIT: datastructure id
      u64 magic_debugging_number; // ATTENTION
      u8 dt[PAGE_SIZE - sizeof(LSN) - sizeof(dt_id) - sizeof(magic_debugging_number)]; // Datastruture BE CAREFUL HERE !!!!!
      // -------------------------------------------------------------------------------------
      operator u8 *()
      {
         return reinterpret_cast<u8 *> (this);
      }
      // -------------------------------------------------------------------------------------
   };
   // -------------------------------------------------------------------------------------
   struct Header header;
   // -------------------------------------------------------------------------------------
   struct Page page; // The persisted part
   // -------------------------------------------------------------------------------------
   bool operator==(const BufferFrame &other)
   {
      return this == &other;
   }
   // -------------------------------------------------------------------------------------
   inline bool isDirty() const
   {
      return header.lastWrittenLSN != page.LSN;
   }
   // -------------------------------------------------------------------------------------
   void reset()
   {
      assert(!header.isWB);
      lock_version_t new_version = header.lock.load();
      new_version += WRITE_LOCK_BIT;
      new_version &= ~WRITE_LOCK_BIT;
      new(&header) Header();
      header.lock = new_version;
   }
};
// -------------------------------------------------------------------------------------
static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(BufferFrame::Page::dt);
// -------------------------------------------------------------------------------------
static_assert(sizeof(BufferFrame::Page) == PAGE_SIZE, "");
// -------------------------------------------------------------------------------------
static_assert((sizeof(BufferFrame) - sizeof(BufferFrame::Page)) == 512, "");
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
