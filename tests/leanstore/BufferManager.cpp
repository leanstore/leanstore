#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/btree/BTreeOptimistic.hpp"
// -------------------------------------------------------------------------------------
#include "gtest/gtest.h"
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
TEST(BufferManager, HelloWorld)
{
   BMC::start();
   BufferManager &bufferManager = *BMC::global_bf;

   auto pid = bufferManager.accquirePage();
   auto &root_bf = bufferManager.accquireBufferFrame();

   auto child_pid = bufferManager.accquirePage();
   auto &child_bf = bufferManager.accquireBufferFrame();
   Swip swizzle(child_pid);
   char *test = "Hello World";
   std::memcpy(child_bf.page, test, 12);
   cout << child_bf.page << endl;

   auto btree_root_pid = bufferManager.accquirePage();
   auto &btree_root_bf = bufferManager.accquireBufferFrame();

   btree::BTree<u64, u64> btree(&btree_root_bf.page);

}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
