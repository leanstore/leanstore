#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
#include "gtest/gtest.h"
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
TEST(BufferManager, HelloWorld)
{
   BufferManager bufferManager;
   auto pid = bufferManager.accquirePage();
   auto &root_bf = bufferManager.accquireBufferFrame();

   auto child_pid = bufferManager.accquirePage();
   auto &child_bf = bufferManager.accquireBufferFrame();
   Swip swizzle(child_pid);
   char *test = "Hello World";
   std::memcpy(child_bf.page, test, 12);
   cout << child_bf.page << endl;
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
