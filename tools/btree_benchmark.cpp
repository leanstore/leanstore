#include "leanstore/LeanStore.hpp"
#include "leanstore/storage/btree/BTreeOptimistic.hpp"
#include "btree/BTree.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
using namespace std;

int main(int argc, char **argv)
{
   uint32_t n = 10e6;
   uint32_t threads = 20;

   std::vector<uint32_t> work(n);
   for ( uint32_t i = 0; i < n; i++ )
      work[i] = i;
   std::random_shuffle(work.begin(), work.end());

   tbb::task_scheduler_init taskScheduler(threads);
   PerfEvent e;
   {
      libgcc::BTree<uint32_t, uint32_t> btree;
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
   }
   {

      optimistic::BTree<uint32_t, uint32_t> btree;
      // insert
      {
         PerfEventBlock b(e, n);
         e.setParam("workload", "insert");
         e.setParam("approach", "optimistic");
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
         e.setParam("approach", "optimistic");
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
         e.setParam("approach", "optimistic");
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
   return 0;
}
