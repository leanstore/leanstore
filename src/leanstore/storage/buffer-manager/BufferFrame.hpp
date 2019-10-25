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
      atomic<u64> LSN = 0;
      atomic<u64> lastWrittenLSN = 0; // TODO: move to the inside of the page
      bool isWB;
      // -------------------------------------------------------------------------------------
      // Swizzling Maintenance
      SwizzlingCallback callback_function = &dummyCallback;
      Swip *swip_in_parent; // the PageID in parent nodes that references to this BF
      PID pid; //not really necessary we can calculate it usings its offset to dram pointer
      // -------------------------------------------------------------------------------------
      OptimisticLock lock;
   };
   struct Header header;
   // -------------------------------------------------------------------------------------
   u8 padding[512 - sizeof(struct Header)];
   // --------------------------------------------------------------------------------
   u8 page[];
   // -------------------------------------------------------------------------------------
   BufferFrame(PID pid);
};
// -------------------------------------------------------------------------------------
static_assert(sizeof(BufferFrame) == 512, "");
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
