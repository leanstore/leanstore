#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct FreeList {
   u64 free_bfs_limit;
   atomic<BufferFrame*> head = {nullptr};
   std::atomic<u64> counter = {0};
   // -------------------------------------------------------------------------------------
   BufferFrame& tryPop();
   void batchPush(BufferFrame* head, BufferFrame* tail, u64 counter);
   void push(BufferFrame& bf);
   FreeList(u64 free_bfs_limit): free_bfs_limit(free_bfs_limit){}
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore

// -------------------------------------------------------------------------------------
