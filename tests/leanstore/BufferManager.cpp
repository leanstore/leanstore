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
   char *test = "Hello World";
   Swip swizzle(0);

   auto &bf = bufferManager.fixPage(swizzle);
   std::memcpy(bf.page, test, 12);
   auto &bf2 = bufferManager.fixPage(swizzle);
   cout << bf2.page << endl;
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
