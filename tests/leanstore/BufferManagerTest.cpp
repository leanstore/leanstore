#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include "gtest/gtest.h"
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
TEST(BufferManager, BTree)
{
   LeanStore db;
   auto &btree = db.registerFSBTree<u32, u32>("test");
   //buffer_manager.stopBackgroundThreads();

   // BTree
   uint32_t result;
   bool res = btree.lookup(10, result);
   EXPECT_FALSE(res);
   u32 tmp = 10;
   btree.insert(10, tmp);
   res = btree.lookup(10, result);
   EXPECT_TRUE(res == true && result == 10);

   uint32_t n = getenv("N") ? atoi(getenv("N")) : 10e9;
   uint32_t threads = getenv("T") ? atoi(getenv("T")) : 10;

   std::vector<uint32_t> work(n);
   for ( uint32_t i = 0; i < n; i++ )
      work[i] = i;
   std::random_shuffle(work.begin(), work.end());
   cout << "initialized workload" << endl;
   tbb::task_scheduler_init taskScheduler(threads);
   PerfEvent e;
   {
      // insert
      {
         PerfEventBlock b(e, n);
         e.setParam("workload", "insert");
         e.setParam("threads", threads);

         tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t> &range) {
            for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
               btree.insert(work[i], work[i]);
            }
         });
      }
      // lookup
      {
         PerfEventBlock b(e, n);
         e.setParam("workload", "lookup");
         e.setParam("threads", threads);

         tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t> &range) {
            for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
               uint32_t result;
               bool success = btree.lookup(work[i], result);
               EXPECT_TRUE(success && result == work[i]);
            }
         });
      }
      // mixed workload
      std::atomic<uint32_t> total(0);
      {
         PerfEventBlock b(e, n);
         e.setParam("workload", "mix");
         e.setParam("threads", threads);

         tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t> &range) {
            uint32_t sum = 0;
            for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
               if ( i % 10 < 6 ) {
                  uint32_t result;
                  bool success = btree.lookup(work[i], result);
                  if ( success ) {
                     EXPECT_TRUE(result == work[i]);
                  }
                  sum += success;
               } else {
                  btree.insert(work[i], work[i]);
               }
            }
            total += sum;
         });
      }
   }
}
// -------------------------------------------------------------------------------------
TEST(BufferManager, Persistence)
{
   return;
   LeanStore db;
   auto &btree = db.registerFSBTree<u32, u32>("test");

   uint32_t n = getenv("N") ? atoi(getenv("N")) : 10e4;
   uint32_t threads = getenv("T") ? atoi(getenv("T")) : 10;

   std::vector<uint32_t> work(n);
   for ( uint32_t i = 0; i < n; i++ )
      work[i] = i;
   std::random_shuffle(work.begin(), work.end());
   cout << "initialized workload" << endl;
   tbb::task_scheduler_init taskScheduler(threads);
   PerfEvent e;
   {

      // insert
      {

         PerfEventBlock b(e, n);
         e.setParam("workload", "insert");
         e.setParam("threads", threads);

         tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t> &range) {
            for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
               btree.insert(work[i], work[i]);
            }
         });
      }
      db.getBufferManager().flushDropAllPages();
      EXPECT_FALSE(btree.root_swip.isSwizzled());

      // mixed workload
      std::atomic<uint32_t> total(0);
      {
         PerfEventBlock b(e, n);
         e.setParam("workload", "mix");
         e.setParam("threads", threads);

         tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t> &range) {
            uint32_t sum = 0;
            for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
               if ( i % 10 < 5 ) {
                  uint32_t result;
                  bool success = btree.lookup(work[i], result);
                  if ( success ) {
                     assert(result == work[i]);
                  }
                  sum += success;
               } else {
                  btree.insert(work[i], work[i]);
               }
            }
            total += sum;
         });
      }
   }
}
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
