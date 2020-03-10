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
  //cout << pin_id << endl;
  // -------------------------------------------------------------------------------------
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(pin_id, &cpuset);
  pthread_t current_thread = pthread_self();
  if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0)
    throw;
}
template <typename T>
inline void DO_NOT_OPTIMIZE(T const& value)
{
#if defined(__clang__)
  asm volatile("" : : "g"(value) : "memory");
#else
  asm volatile("" : : "i,r,m"(value) : "memory");
#endif
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
  struct alignas(64) BF {
    mutex lock;
    buffermanager::OptimisticLatch version;
    u64 seq_id = 0;
  };
  BF bf;
  const u64 max_size = FLAGS_samples_count;
  s32* sequence = new s32[max_size];
  for (s32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
    threads.emplace_back(
        [&](int t_i) {
          //pinme();
          while (keep_running) {
            if (FLAGS_mutex) {
              bf.lock.lock();
              sequence[bf.seq_id] = t_i;
              bf.seq_id = (bf.seq_id + 1) % max_size;
              tx_counter[t_i]++;
              bf.lock.unlock();
            } else {
              jumpmuTry() {
                OptimisticGuard guard(bf.version);
                ExclusiveGuard ex_guard(guard);
                sequence[bf.seq_id] = t_i;
                bf.seq_id = (bf.seq_id + 1) % max_size;
                tx_counter[t_i]++;
              }
              jumpmuCatch() {

              }
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
  sleep(FLAGS_run_for_seconds);
  keep_running = false;
  for (auto& thread : threads) {
    thread.join();
  }
  threads.clear();
  std::ofstream csv;
  csv.open(FLAGS_csv_path.c_str(), ios::app);
  csv.seekp(0, ios::end);
  csv << std::setprecision(2) << std::fixed;
  if (csv.tellp() == 0) {
    csv << "i,t,c_worker_threads,c_mutex,c_backoff" << endl;
  }
  for (u64 i = 0; i < max_size; i++) {
    csv << i << "," << sequence[i] << "," << FLAGS_worker_threads << "," << FLAGS_mutex << "," << FLAGS_x << endl;
  }
  return 0;
}
