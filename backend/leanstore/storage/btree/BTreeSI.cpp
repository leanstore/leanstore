#include "BTree.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::storage::btree::OP_RESULT;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
void BTree::undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts)
{
   if (FLAGS_vi) {
      undoVI(btree_object, wal_entry_ptr, tts);
   } else if (FLAGS_vw) {
      undoVW(btree_object, wal_entry_ptr, tts);
   } else {
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
