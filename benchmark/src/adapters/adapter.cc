#include "benchmark/adapters/adapter.h"

#include "share_headers/logger.h"
#include "share_headers/time.h"

#include <ranges>

auto BaseDatabase::StartProfilingThread(const std::string &system_name, std::atomic<bool> &keep_running,
                                        std::atomic<uint64_t> &completed_txn) -> std::thread {
  return std::thread([&]() {
    uint64_t cnt = 0;
    std::printf("system,ts,tx\n");
    while (keep_running.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      auto progress = completed_txn.exchange(0);
      total_txn_completed += progress;
      std::printf("%s,%lu,%lu\n", system_name.c_str(), cnt++, progress);
    }
    std::printf("Halt Profiling thread\n");
  });
}

void BaseDatabase::Report(uint64_t tid, uint64_t start_time) {
  latencies[tid].emplace_back(tsctime::TscDifferenceNs(start_time, tsctime::ReadTSC()));
}

void BaseDatabase::LatencyEvaluation() {
  for (auto idx = 1U; idx < latencies.size(); idx++) {
    std::ranges::copy(latencies[idx], std::back_inserter(latencies[0]));
  }
  LOG_DEBUG("# data points: %lu", latencies[0].size());
  LOG_DEBUG("Start evaluating latency data");
  std::sort(latencies[0].begin(), latencies[0].end());
  LOG_DEBUG("Statistics:\n\tMedianLat(%.4f us)\n\t90thLat(%.4f us)\n\t99thLat(%.4f us)\n\t99.9thLat(%.4f us)",
            static_cast<float>(latencies[0][latencies[0].size() / 2 - 1]) / 1000UL,
            static_cast<float>(latencies[0][latencies[0].size() * 0.9 - 1]) / 1000UL,
            static_cast<float>(latencies[0][latencies[0].size() * 0.99 - 1]) / 1000UL,
            static_cast<float>(latencies[0][latencies[0].size() * 0.999 - 1]) / 1000UL);
}
