#pragma once
#include "FreeList.hpp"
#include "BufferFrame.hpp"
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
struct CIOFrame {
  enum class State : u8 {
    READING = 0,
    COOLING = 1,
    UNDEFINED = 2  // for debugging
  };
  std::mutex mutex;
  std::list<BufferFrame*>::iterator fifo_itr;
  State state = State::UNDEFINED;
  // -------------------------------------------------------------------------------------
  // Everything in CIOFrame is protected by global bf_s_lock except the
  // following counter
  atomic<s64> readers_counter = 0;
};
// -------------------------------------------------------------------------------------
struct HashTable {
  struct Entry {
    u64 key;
    Entry* next;
    CIOFrame value;
    Entry(u64 key);
  };
  // -------------------------------------------------------------------------------------
  struct Handler {
    Entry** holder;
    operator bool() const { return holder != nullptr; }
    CIOFrame& frame() const
    {
      assert(holder != nullptr);
      return *reinterpret_cast<CIOFrame*>(&((*holder)->value));
    }
  };
  // -------------------------------------------------------------------------------------
  u64 mask;
  Entry** entries;
  // -------------------------------------------------------------------------------------
  u64 hashKey(u64 k);
  CIOFrame& insert(u64 key);
  Handler lookup(u64 key);
  void remove(Handler& handler);
  void remove(u64 key);
  bool has(u64 key);  // for debugging
  HashTable(u64 size_in_bits);
};
// -------------------------------------------------------------------------------------
struct Partition {
  std::mutex cio_mutex;
  HashTable ht;
  std::list<BufferFrame*> cooling_queue;
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
