#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
struct FreeList {
   atomic<BufferFrame*> head = nullptr;
   atomic<u64> counter = 0;
   BufferFrame& tryPop(JMUW<std::unique_lock<std::mutex>>& lock);
   BufferFrame& pop();
   void push(BufferFrame& bf);
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore

// -------------------------------------------------------------------------------------
