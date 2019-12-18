#include "leanstore/utils/RandomGenerator.hpp"
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
  uint32_t n = getenv("N") ? atoi(getenv("N")) : 1024 * 128;
  uint32_t repeat = getenv("R") ? atoi(getenv("N")) : 10000;
  uint32_t threads = getenv("T") ? atoi(getenv("T")) : 20;
  atomic<uint64_t> data[n];
  for (uint64_t i = 0; i < n; i++) {
    data[i] = 0;
  }

  tbb::task_scheduler_init taskScheduler(threads);
  PerfEvent e;
  {
    e.setParam("variant", "mtv");
    PerfEventBlock b(e, n * repeat);
    for (uint64_t r = 0; r < repeat; r++)
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
        for (u64 i = range.begin(); i < range.end(); i++) {
          data[i] += leanstore::utils::RandomGenerator::getRand(0, 100);
        }
      });
  }

  {
    e.setParam("variant", "std");
    PerfEventBlock b(e, n * repeat);
    for (uint64_t r = 0; r < repeat; r++)
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
        for (u64 i = range.begin(); i < range.end(); i++) {
          data[i] += leanstore::utils::RandomGenerator::getRandU64STD(0, 100);
        }
      });
  }
  for (uint8_t i = 0; i < 10; i++)
    cout << leanstore::utils::RandomGenerator::getRand(0, 3) << endl;
  return 0;
}