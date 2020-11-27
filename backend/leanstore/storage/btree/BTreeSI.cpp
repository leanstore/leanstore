#include "BTree.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::storage::btree::BTree::OP_RESULT;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
void BTree::applyDelta(u8* dst, u8* delta_beginning, u16 delta_size)
{
   u8* delta_ptr = delta_beginning;
   while (delta_ptr - delta_beginning < delta_size) {
      const u16 offset = *reinterpret_cast<u16*>(delta_ptr);
      delta_ptr += 2;
      const u16 size = *reinterpret_cast<u16*>(delta_ptr);
      delta_ptr += 2;
      std::memcpy(dst + offset, delta_ptr, size);
      delta_ptr += size;
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
