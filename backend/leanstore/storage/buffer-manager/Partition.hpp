#pragma once
#include "BufferFrame.hpp"
#include "FreeList.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <list>
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
struct IOFrame {
  enum class STATE : u8 {
    READING = 0,
    READY = 1,
    UNDEFINED = 2  // for debugging
  };
  std::mutex mutex;
  STATE state = STATE::UNDEFINED;
  BufferFrame* bf = nullptr;
  // -------------------------------------------------------------------------------------
  // Everything in CIOFrame is protected by partition lock
  // except the following counter which is decremented outside to determine
  // whether it is time to remove it
  atomic<s64> readers_counter = 0;
};
// -------------------------------------------------------------------------------------
struct HashTable {
  struct Entry {
    u64 key;
    Entry* next;
    IOFrame value;
    Entry(u64 key);
  };
  // -------------------------------------------------------------------------------------
  struct Handler {
    Entry** holder;
    operator bool() const { return holder != nullptr; }
    IOFrame& frame() const
    {
      assert(holder != nullptr);
      return *reinterpret_cast<IOFrame*>(&((*holder)->value));
    }
  };
  // -------------------------------------------------------------------------------------
  u64 mask;
  Entry** entries;
  // -------------------------------------------------------------------------------------
  u64 hashKey(u64 k);
  IOFrame& insert(u64 key);
  Handler lookup(u64 key);
  void remove(Handler& handler);
  void remove(u64 key);
  bool has(u64 key);  // for debugging
  HashTable(u64 size_in_bits);
};
// -------------------------------------------------------------------------------------
struct Partition {
  std::mutex io_mutex;
  HashTable ht;
  // -------------------------------------------------------------------------------------
  std::mutex cooling_mutex;
  std::list<BufferFrame*> cooling_queue;
  // -------------------------------------------------------------------------------------
  atomic<u64> cooling_bfs_counter = 0;
  const u64 free_bfs_limit;
  const u64 cooling_bfs_limit;
  FreeList dram_free_list;
  Partition(u64 free_bfs_limit, u64 cooling_bfs_limit);
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
