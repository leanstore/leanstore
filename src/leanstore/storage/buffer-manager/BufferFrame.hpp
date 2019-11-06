#pragma once
#include "Units.hpp"
#include "Swip.hpp"
#include "leanstore/sync-primitives/OptimisticLock.hpp"
#include "leanstore/storage/catalog/DtRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore {
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
      PID pid; // INIT:
      OptimisticVersion lock = 0;  // INIT:
   };
   struct alignas(512) Page {
      u64 LSN = 0;
      DTType dt_type; //INIT: datastructure id TODO
      u8 dt[ PAGE_SIZE - sizeof(LSN) - sizeof(dt_type)]; // Datastruture
      // -------------------------------------------------------------------------------------
      operator u8*() {
         assert(false);
      }
      // -------------------------------------------------------------------------------------
   };
   // -------------------------------------------------------------------------------------
   struct Header header;
   // -------------------------------------------------------------------------------------
   struct Page page; // The persisted part
   // -------------------------------------------------------------------------------------
   BufferFrame(PID pid = 0);
};
// -------------------------------------------------------------------------------------
static_assert((sizeof(BufferFrame) - sizeof(BufferFrame::Page)) == 512, "");
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
