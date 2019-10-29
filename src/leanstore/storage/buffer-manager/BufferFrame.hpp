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
enum class SwizzlingCallbackCommand : u8 {
   ITERATE,
   PARENT
};
// -------------------------------------------------------------------------------------
using SwizzlingCallback = std::vector<Swip*> (*)(u8 *payload, SwizzlingCallbackCommand command);
// -------------------------------------------------------------------------------------
std::vector<Swip*> dummyCallback(u8* payload, SwizzlingCallbackCommand command);
// -------------------------------------------------------------------------------------
struct BufferFrame {
   struct Header {
      // TODO: for logging
      atomic<u64> lastWrittenLSN = 0; // TODO: move to the inside of the page
      bool isWB = false;
      // -------------------------------------------------------------------------------------
      // Swizzling Maintenance
      SwizzlingCallback callback_function = &dummyCallback;
      // nullptr means the bufferframe is sticky, e.g. BTree root
      PID pid; //not really necessary we can calculate it usings its offset to dram pointer
      // -------------------------------------------------------------------------------------
      OptimisticLock lock = 0;
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
