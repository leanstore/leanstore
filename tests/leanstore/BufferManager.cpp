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
   Swizzle swizzle(0);

   auto &bf = bufferManager.fixPage(swizzle);
   std::memcpy(bf.payload, test, 12);
   auto &bf2 = bufferManager.fixPage(swizzle);

   cout << bf2.payload << endl;
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
