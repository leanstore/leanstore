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
DEFINE_bool(mutex, false, "");
DEFINE_bool(cmpxchg, false, "");
DEFINE_bool(seq, false, "");
// -------------------------------------------------------------------------------------
void pinme()
{
  static atomic<u64> a_t_i = 0;
  u64 t_i = a_t_i++;
  u64 cpu = t_i / 8;
  u64 l_cpu = t_i % 8;
  bool is_upper = l_cpu > 3;
  u64 pin_id = (is_upper) ? (64 + (cpu * 4) + (l_cpu % 4)) : ((cpu * 4) + (l_cpu % 4));
  // -------------------------------------------------------------------------------------
  // cout << pin_id << endl;
  // -------------------------------------------------------------------------------------
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(pin_id, &cpuset);
  pthread_t current_thread = pthread_self();
  if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0)
    throw;
}
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
  using AtomicType = u64;
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
            AtomicType c = e | 1;
            if (bf.version.compare_exchange_strong(e, c)) {
              tx_counter[t_i]++;
              bf.version = 0;
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
