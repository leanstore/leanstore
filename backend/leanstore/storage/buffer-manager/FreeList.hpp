#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
#include "leanstore/concurrency/Mean.hpp"
#include <mutex>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct FreeList {
   atomic<BufferFrame*> head = nullptr;
   atomic<u64> counter = 0;
   BufferFrame& tryPop(JMUW<std::unique_lock<mean::mutex>>& lock);
   BufferFrame& pop();
   void batchPush(BufferFrame* head, BufferFrame* tail, u64 counter);
   void push(BufferFrame& bf);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore

// -------------------------------------------------------------------------------------
