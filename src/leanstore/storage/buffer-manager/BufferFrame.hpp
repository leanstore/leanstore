#pragma once
#include "Units.hpp"
#include "Swizzle.hpp"
#include "../optimistic-lock/OptimisticLock.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
enum class BufferFrameType : u8 {
   BTREE
};
// -------------------------------------------------------------------------------------
enum class SwizzlingCallbackCommand : u8 {
   ITERATE,
   PARENT
};
// -------------------------------------------------------------------------------------
using SwizzlingCallback = std::vector<Swizzle*> (*)(u8 *payload);
// -------------------------------------------------------------------------------------
std::vector<Swizzle*> dummyCallback(u8* payload) {
   return {};
}
// -------------------------------------------------------------------------------------
struct BufferFrame {
   bool dirty = false;
   bool fixed = false;
   // -------------------------------------------------------------------------------------
   // Swizzling Maintenance
   BufferFrameType type;
   SwizzlingCallback callback_function = &dummyCallback;
   Swizzle *parent_pointer; // the PageID in parent nodes that references to this BF
   PID pid; //not really necessary we can calculate it usings its offset to dram pointer
   // -------------------------------------------------------------------------------------
   OptimisticLock lock;
   // -------------------------------------------------------------------------------------
   u8 padding[512 - 40];
   u8 payload[];
   // -------------------------------------------------------------------------------------
   BufferFrame(PID pid);
};
// -------------------------------------------------------------------------------------
static_assert(sizeof(BufferFrame) == 512, "");
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
