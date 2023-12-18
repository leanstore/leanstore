#include "IoAbstraction.hpp"
#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <sys/mman.h>
#include <stdexcept>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
// Env
// -------------------------------------------------------------------------------------
/*
void* IoEnvironment::allocIoMemoryChecked(size_t size, size_t align)
{
   auto mem = allocIoMemory(size, align);
   null_check(mem, "Memory allocation failed");
   return mem;
};
void* IoEnvironment::allocIoMemory(size_t size, [[maybe_unused]]size_t align)
{
   void* bfs = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
   madvise(bfs, size, MADV_HUGEPAGE);
   madvise(bfs, size,
           MADV_DONTFORK);  // O_DIRECT does not work with forking.
   return bfs;
   // return std::aligned_alloc(align, size);
}
void IoEnvironment::freeIoMemory(void* ptr, size_t size)
{
   munmap(ptr, size);
   // std::free(ptr);
}
*/
void* RaidEnvironment::allocIoMemoryChecked(size_t size, size_t align)
{
   auto mem = allocIoMemory(size, align);
   null_check(mem, "Memory allocation failed");
   return mem;
};

// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
