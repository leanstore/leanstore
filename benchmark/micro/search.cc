#include "storage/extent/extent_list.h"

#include "benchmark/benchmark.h"

#include <algorithm>
#include <random>
#include <ranges>
#include <vector>

static void BM_NormLinearSearch(benchmark::State &state) {
  for (auto _ : state) {
    auto num  = rand() % leanstore::storage::ExtentList::MAX_EXTENT_SIZE + 1;
    auto comp = [&num](u32 other) { return other < num; };
    std::ranges::find_if_not(leanstore::storage::ExtentList::TOTAL_TIER_SIZE, comp);
  }
}

static void BM_LinearSearch(benchmark::State &state) {
  for (auto _ : state) {
    auto num  = rand() % leanstore::storage::ExtentList::MAX_EXTENT_SIZE + 1;
    auto comp = [&num](u32 other) { return other < num; };
    benchmark::DoNotOptimize(std::ranges::find_if_not(leanstore::storage::ExtentList::TOTAL_TIER_SIZE, comp));
  }
}

static void BM_NormBinarySearch(benchmark::State &state) {
  for (auto _ : state) {
    auto num = rand() % leanstore::storage::ExtentList::MAX_EXTENT_SIZE + 1;
    std::ranges::lower_bound(leanstore::storage::ExtentList::TOTAL_TIER_SIZE, num);
  }
}

static void BM_BinarySearch(benchmark::State &state) {
  for (auto _ : state) {
    auto num = rand() % leanstore::storage::ExtentList::MAX_EXTENT_SIZE + 1;
    benchmark::DoNotOptimize(std::ranges::lower_bound(leanstore::storage::ExtentList::TOTAL_TIER_SIZE, num));
  }
}

BENCHMARK(BM_LinearSearch);
BENCHMARK(BM_BinarySearch);
BENCHMARK(BM_NormLinearSearch);
BENCHMARK(BM_NormBinarySearch);

BENCHMARK_MAIN();