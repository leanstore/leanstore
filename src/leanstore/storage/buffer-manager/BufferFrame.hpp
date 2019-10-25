#pragma once
#include "Units.hpp"
#include "Swizzle.hpp"
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
using SwizzlingCallback = std::vector<Swizzle*> (*)(u8 *payload, SwizzlingCallbackCommand command);
// -------------------------------------------------------------------------------------
std::vector<Swizzle*> dummyCallback(u8* payload, SwizzlingCallbackCommand command);
// -------------------------------------------------------------------------------------
struct BufferFrame {
   struct Header {
      bool dirty = false;
      bool fixed = false;
      // -------------------------------------------------------------------------------------
      // Swizzling Maintenance
      SwizzlingCallback callback_function = &dummyCallback;
      Swizzle *parent_pointer; // the PageID in parent nodes that references to this BF
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
