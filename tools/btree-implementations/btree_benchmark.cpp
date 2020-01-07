#include "btree-exceptions/BTree.hpp"
#include "btree-goto/BTree.hpp"
#include "btree-jmu/BTree.hpp"
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace std;
template <typename BTreeType>
void bench(string name, const std::vector<u64>& work, u64 t, PerfEvent& e)
{
  BTreeType btree;
  const u64 n = work.size();
  // insert
  {
    PerfEventBlock b(e, n);
    e.setParam("workload", "insert");
    e.setParam("approach", name);
    e.setParam("threads", t);

    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      for (u64 i = range.begin(); i < range.end(); i++) {
        btree.insert(work[i], work[i]);
      }
    });
  }
  // lookup
  {
    PerfEventBlock b(e, n);
    e.setParam("workload", "lookup");
    e.setParam("approach", name);
    e.setParam("threads", t);

    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      for (u64 i = range.begin(); i < range.end(); i++) {
        u64 result;
        bool success = btree.lookup(work[i], result);
        assert(success);
        assert(result == work[i]);
      }
    });
  }
  // mixed workload
  std::atomic<u64> total(0);
  {
    PerfEventBlock b(e, n);
    e.setParam("workload", "mix");
    e.setParam("approach", name);
    e.setParam("threads", t);

    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
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
struct TestObject {
  TestObject(){
    cout <<"con"<<endl;
  }
  ~TestObject(){
    cout <<"des"<<endl;
  }
};
// -------------------------------------------------------------------------------------
int main(int, char**)
{
  u64 n = getenv("N") ? atoi(getenv("N")) : 1e6;
  u64 t = getenv("T") ? atoi(getenv("T")) : 4;
  // -------------------------------------------------------------------------------------
  PerfEvent e;
  // -------------------------------------------------------------------------------------
  tbb::task_scheduler_init taskScheduler(t);
  std::vector<u64> work(n);
  tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
    for (u64 i = range.begin(); i < range.end(); i++) {
      work[i] = i;
    }
  });
  std::random_shuffle(work.begin(), work.end());
  cout << "start" << endl;
  // -------------------------------------------------------------------------------------
  bench<btree::uglygoto::BTree<u64, u64>>("goto", work, t, e); // goto implementation is still buggy
  bench<btree::libgcc::BTree<u64, u64>>("libgcc", work, t, e);
  bench<btree::jmu::BTree<u64, u64>>("jumpmu", work, t, e);
  return 0;
}
