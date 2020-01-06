#include "btree-legacy/BTreeOptimistic.hpp"
#include "btree/BTree.hpp"
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
  uint32_t n = getenv("N") ? atoi(getenv("N")) : 1e6;
  uint32_t threads = getenv("T") ? atoi(getenv("T")) : 20;

  std::vector<u64> work(n);
  for (u64 i = 0; i < n; i++)
    work[i] = i;
  std::random_shuffle(work.begin(), work.end());

  tbb::task_scheduler_init taskScheduler(threads);
  PerfEvent e;
     {
        libgcc::BTree<uint32_t, uint32_t> btree;

        // insert
        {
           PerfEventBlock b(e, n);
           e.setParam("workload", "insert");
           e.setParam("approach", "jumpmu");
           e.setParam("threads", threads);

           tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t> &range) {
              for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
                 btree.insert(work[i], work[i]);
              }
           });
        }
        cout << btree.restarts_counter.load() << endl;
        if(getenv("ALL")){
           // lookup
           {
              PerfEventBlock b(e, n);
              e.setParam("workload", "lookup");
              e.setParam("approach", "jumpmu");
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
              e.setParam("approach", "jumpmu");
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
  {
    optimistic::BTree<u64, u64> btree;
    // insert
    {
      PerfEventBlock b(e, n);
      e.setParam("workload", "insert");
      e.setParam("approach", "optimistic");
      e.setParam("threads", threads);

      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t>& range) {
        for (uint32_t i = range.begin(); i < range.end(); i++) {
          btree.insert(work[i], work[i]);
        }
      });
    }

    if (getenv("ALL")) {
      // lookup
      {
        PerfEventBlock b(e, n);
        e.setParam("workload", "lookup");
        e.setParam("approach", "optimistic");
        e.setParam("threads", threads);

        tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
          for (u64 i = range.begin(); i < range.end(); i++) {
            u64 result;
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

        tbb::parallel_for(tbb::blocked_range<uint32_t>(0, n), [&](const tbb::blocked_range<uint32_t>& range) {
          u64 sum = 0;
          for (u64 i = range.begin(); i < range.end(); i++) {
            if (i % 10 < 6) {
              u64 result;
              bool success = btree.lookup(work[i], result);
              if (success) {
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
  return 0;
}
