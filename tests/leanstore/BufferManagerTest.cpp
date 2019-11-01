#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/btree/BTreeOptimistic.hpp"
// -------------------------------------------------------------------------------------
#include "gtest/gtest.h"
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
TEST(BufferManager, BTree)
{
   BMC::start();
   BufferManager &buffer_manager = *BMC::global_bf;
   buffer_manager.stopBackgroundThreads();

   // BTree
   auto &btree_root_bf = buffer_manager.allocatePage();
   btree::BTree<uint32_t, uint32_t> btree(&btree_root_bf);
   btree.init();
   btree_root_bf.header.lock.fetch_add(2);

   uint32_t result;
   bool res = btree.lookup(10, result);
   assert(res == false);
   btree.insert(10, 10);
   res = btree.lookup(10, result);
   assert(res == true && result == 10);

   uint32_t n = getenv("N") ? atoi(getenv("N")) : 10e4;
   uint32_t threads = getenv("T") ? atoi(getenv("T")) : 10;

   std::vector<uint32_t> work(n);
   for ( uint32_t i = 0; i < n; i++ )
      work[i] = i;
   std::random_shuffle(work.begin(), work.end());
   tbb::task_scheduler_init taskScheduler(threads);
   PerfEvent e;
   {
      // insert
      {
         PerfEventBlock b(e, n);
         e.setParam("workload", "insert");
         e.setParam("approach", "libgcc_opt");
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
         e.setParam("approach", "libgcc_opt");
         e.setParam("threads", threads);

         tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t> &range) {
            for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
               uint32_t result;
               bool success = btree.lookup(work[i], result);
               assert(success && result == work[i]);
            }
         });
      }
      // mixed workload
      std::atomic<uint32_t> total(0);
      {
         PerfEventBlock b(e, n);
         e.setParam("workload", "mix");
         e.setParam("approach", "libgcc_opt");
         e.setParam("threads", threads);

         tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t> &range) {
            uint32_t sum = 0;
            for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
               if ( i % 10 < 6 ) {
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
// -------------------------------------------------------------------------------------
