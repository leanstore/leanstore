#pragma once
#include "BufferFrame.hpp"
#include "FreeList.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct IOFrame {
   enum class STATE : u8 {
      READING = 0,
      READY = 1,
      TO_DELETE = 2,
      UNDEFINED = 3  // for debugging
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
   HashTable io_ht;
   // -------------------------------------------------------------------------------------
   std::mutex next_mutex, good_mutex;
   std::vector<BufferFrame*> next_queue, good_queue;
   std::atomic<WATT_TIME> last_good_check = {0};
   std::atomic<bool> is_page_provided = {false};
   const u64 max_partition_size;
   atomic<u64> partition_size = 0;
   const u64 next_bfs_limit;
   // -------------------------------------------------------------------------------------
   // SSD Pages
   const u64 pid_distance;
   std::mutex pids_mutex;  // protect free pids vector
   std::vector<PID> freed_pids;
   u64 next_pid;
   inline PID nextPID()
   {
      std::unique_lock<std::mutex> g_guard(pids_mutex);
      partition_size++;
      if (freed_pids.size()) {
         const u64 pid = freed_pids.back();
         freed_pids.pop_back();
         return pid;
      } else {
         const u64 pid = next_pid;
         next_pid += pid_distance;
         ensure((pid * PAGE_SIZE / 1024 / 1024 / 1024) <= FLAGS_ssd_gib);
         return pid;
      }
   }
   void freePage(PID pid)
   {
      std::unique_lock<std::mutex> g_guard(pids_mutex);
      freed_pids.push_back(pid);
   }
   u64 allocatedPages() { return next_pid / pid_distance; }
   u64 freedPages()
   {
      std::unique_lock<std::mutex> g_guard(pids_mutex);
      return freed_pids.size();
   }
   void addNextBufferFrame(BufferFrame* next);
   void addGoodBufferFrame(BufferFrame* good);
   // -------------------------------------------------------------------------------------
   Partition(u64 first_pid, u64 pid_distance, u64 max_partition_size, u64 next_bfs_limit);
  private:
   void addToList(std::mutex& mutex, std::vector<BufferFrame*>& list, BufferFrame* next);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
