#pragma once

#include "share_headers/time.h"

#include <functional>
#include <random>

namespace benchmark {

class PoissonScheduler {
 public:
  explicit PoissonScheduler(double txn_per_sec);
  ~PoissonScheduler() = default;

  auto IsEnable() -> bool;
  auto Wait(const std::function<void()> &idle_fn) -> uint64_t;

 private:
  static thread_local uint64_t prev_tsc;
  static thread_local std::mt19937 generator;
  uint64_t rate_;
  std::exponential_distribution<> dist_;
};

}  // namespace benchmark
