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
// -------------------------------------------------------------------------------------
const u64 PAGE_SIZE = 16 * 1024;
// -------------------------------------------------------------------------------------
struct BufferFrame {
   struct Header {
      // TODO: for logging
      atomic<u64> lastWrittenLSN = 0;
      bool isWB = false;
      PID pid; //not really necessary we can calculate it usings its offset to dram pointer
      // -------------------------------------------------------------------------------------
      OptimisticVersion lock = 0;
   };
   struct alignas(512) Page {
      atomic<u64> LSN = 0;
      u32 dt_id; //datastructure id TODO
      u8 dt[]; // Datastruture
      operator u8*() {
         return reinterpret_cast<u8*>(this);
      }
   };
   // -------------------------------------------------------------------------------------
   struct Header header;
   // -------------------------------------------------------------------------------------
   struct Page page; // The persisted part
   // -------------------------------------------------------------------------------------
   BufferFrame(PID pid);
   BufferFrame(){}
};
// -------------------------------------------------------------------------------------
static_assert((sizeof(BufferFrame) - sizeof(BufferFrame::Page)) == 512, "");
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
