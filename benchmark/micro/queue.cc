#include "common/concurrent_queue.h"
#include "common/utils.h"

#include "benchmark/benchmark.h"
#include "boost/lockfree/queue.hpp"
#include "tbb/concurrent_queue.h"

#define NO_ELEMENTS 1000
#define NO_THREADS 10

static void BM_DequeWithLatch(benchmark::State &state) {
  leanstore::ConcurrentQueue<int> queue;
  std::vector<std::thread> workers;

  for (auto _ : state) {
    workers.clear();

    for (int x = 0; x < NO_THREADS; x++) {
      workers.emplace_back([&, x]() {
        int element = 0;

        for (int i = 0; i < NO_ELEMENTS; i++) {
          while (!queue.Dequeue(element)) { leanstore::AsmYield(); }
        }
      });
    }

    for (int x = 0; x < NO_THREADS; x++) {
      workers.emplace_back([&, x]() {
        for (int i = 0; i < NO_ELEMENTS; i++) { queue.Enqueue(i); }
      });
    }

    for (auto &worker : workers) { worker.join(); }
  }
}

static void BM_TBBQueue(benchmark::State &state) {
  tbb::concurrent_queue<int> queue;

  std::vector<std::thread> workers;

  for (auto _ : state) {
    workers.clear();

    for (int x = 0; x < NO_THREADS; x++) {
      workers.emplace_back([&, x]() {
        int element = 0;

        for (int i = 0; i < NO_ELEMENTS; i++) {
          while (!queue.try_pop(element)) { leanstore::AsmYield(); }
        }
      });
    }

    for (int x = 0; x < NO_THREADS; x++) {
      workers.emplace_back([&, x]() {
        for (int i = 0; i < NO_ELEMENTS; i++) { queue.push(i); }
      });
    }

    for (auto &worker : workers) { worker.join(); }
  }
}

static void BM_BoostQueue(benchmark::State &state) {
  boost::lockfree::queue<int> queue(128);

  std::vector<std::thread> workers;

  for (auto _ : state) {
    workers.clear();

    for (int x = 0; x < NO_THREADS; x++) {
      workers.emplace_back([&, x]() {
        int element = 0;

        for (int i = 0; i < NO_ELEMENTS; i++) {
          while (!queue.pop(element)) { leanstore::AsmYield(); }
        }
      });
    }

    for (int x = 0; x < NO_THREADS; x++) {
      workers.emplace_back([&, x]() {
        for (int i = 0; i < NO_ELEMENTS; i++) { queue.push(i); }
      });
    }

    for (auto &worker : workers) { worker.join(); }
  }
}

BENCHMARK(BM_DequeWithLatch);
BENCHMARK(BM_TBBQueue);
BENCHMARK(BM_BoostQueue);

BENCHMARK_MAIN();