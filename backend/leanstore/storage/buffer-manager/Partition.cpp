#include "Partition.hpp"

#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <sys/mman.h>

#include <cstring>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void* malloc_huge(size_t size)
{
   void* p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_HUGEPAGE);
   memset(p, 0, size);
   return p;
}
// -------------------------------------------------------------------------------------
HashTable::Entry::Entry(PID key) : key(key) {}
// -------------------------------------------------------------------------------------
HashTable::HashTable(u64 sizeInBits)
{
   uint64_t size = (1ull << sizeInBits);
   mask = size - 1;
   entries = (Entry**)malloc_huge(size * sizeof(Entry*));
}
// -------------------------------------------------------------------------------------
u64 HashTable::hashKey(PID k)
{
   // MurmurHash64A
   const uint64_t m = 0xc6a4a7935bd1e995ull;
   const int r = 47;
   uint64_t h = 0x8445d61a4e774912ull ^ (8 * m);
   k *= m;
   k ^= k >> r;
   k *= m;
   h ^= k;
   h *= m;
   h ^= h >> r;
   h *= m;
   h ^= h >> r;
   return h;
}
// -------------------------------------------------------------------------------------
IOFrame& HashTable::insert(PID key)
{
   auto e = new Entry(key);
   uint64_t pos = hashKey(key) & mask;
   e->next = entries[pos];
   entries[pos] = e;
   return e->value;
}
// -------------------------------------------------------------------------------------
HashTable::Handler HashTable::lookup(PID key)
{
   uint64_t pos = hashKey(key) & mask;
   Entry** e_ptr = entries + pos;
   Entry* e = *e_ptr;  // e is only here for readability
   while (e) {
      if (e->key == key)
         return {e_ptr};
      e_ptr = &(e->next);
      e = e->next;
   }
   return {nullptr};
}
// -------------------------------------------------------------------------------------
void HashTable::remove(HashTable::Handler& handler)
{
   Entry* to_delete = *handler.holder;
   *handler.holder = (*handler.holder)->next;
   delete to_delete;
}
// -------------------------------------------------------------------------------------
void HashTable::remove(u64 key)
{
   auto handler = lookup(key);
   assert(handler);
   remove(handler);
}
// -------------------------------------------------------------------------------------
bool HashTable::has(u64 key)
{
   uint64_t pos = hashKey(key) & mask;
   auto e = entries[pos];
   while (e) {
      if (e->key == key)
         return true;
      e = e->next;
   }
   return false;
}
// -------------------------------------------------------------------------------------
Partition::Partition(u64 first_pid, u64 pid_distance, u64 free_bfs_limit, u64 cooling_bfs_limit)
    : io_ht(utils::getBitsNeeded(cooling_bfs_limit)), free_bfs_limit(free_bfs_limit), cooling_bfs_limit(cooling_bfs_limit), pid_distance(pid_distance)
{
   next_pid = first_pid;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
