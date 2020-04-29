#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/sync-primitives/OptimisticLock.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <emmintrin.h>
#include <fcntl.h>
#include <linux/futex.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(samples_count, (1 << 20), "");
DEFINE_bool(futex, false, "");
DEFINE_bool(cmpxchg, false, "");
DEFINE_bool(seq, false, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
using namespace leanstore::buffermanager;
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  ensure(FLAGS_worker_threads > 0);
  vector<thread> threads;
  atomic<bool> keep_running = true;
  alignas(64) atomic<u64> tx_counter[FLAGS_worker_threads] = {0};
  // -------------------------------------------------------------------------------------
  atomic<u64> counter = 0;
  atomic<u64> sleep_counter = 0;
  std::array<u8, 128> payload = {0};
  std::array<u8, 128> dump = {1};
  // -------------------------------------------------------------------------------------
  using AtomicType = u128;
  struct alignas(64) BF {
    atomic<AtomicType> version = 0;
  };
  BF bf;
  for (s32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
    threads.emplace_back(
        [&](int t_i) {
          while (keep_running) {
            AtomicType e = bf.version.load();
            while (e & 1) {
              _mm_pause();
              e = bf.version.load();
            }
            AtomicType c = e + 1;
            if (bf.version.compare_exchange_strong(e, c)) {
              std::memcpy(dump.data(), payload.data(), 128);
              tx_counter[t_i]++;
              bf.version = c + 1;
            }
          }
        },
        t_i);
  }
  threads.emplace_back([&]() {
    while (keep_running) {
      u64 tx_sum = 0;
      for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++)
        tx_sum += tx_counter[t_i].exchange(0);
      cout << tx_sum / 1.0e6 << "\t" << sleep_counter.exchange(0) / 1.0e6 << endl;
      sleep(1);
    }
  });
  for (auto& thread : threads) {
    thread.join();
  }
  return 0;
}
