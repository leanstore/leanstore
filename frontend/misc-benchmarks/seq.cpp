#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/sync-primitives/OptimisticLock.hpp"
#include "leanstore/utils/Misc.hpp"
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
DEFINE_string(type, "default", "");
DEFINE_bool(seq, false, "");
DEFINE_bool(ticket_shared_cl, false, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
using namespace leanstore::buffermanager;
int main(int argc, char** argv)
{
  // -------------------------------------------------------------------------------------
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
    buffermanager::HybridLatch version;
    std::mutex lock;
    u64 seq_id = 0;
    // -------------------------------------------------------------------------------------
    atomic<u64> turn = 0;
  };
  struct alignas(64) AUX {
    atomic<u64> ticket = 0;
  };
  BF bf;
  AUX aux;
  const u64 max_size = FLAGS_samples_count;
  s32* sequence = new s32[max_size];
  PerfEvent e;
  e.printCSVHeaders(cout);
  for (s32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
    threads.emplace_back([&, t_i]() {
      if (FLAGS_pin_threads)
        utils::pinThisThreadRome(t_i);
      while (keep_running) {
        if (FLAGS_type == "mutex") {
          bf.lock.lock();
          sequence[bf.seq_id] = t_i;
          bf.seq_id = (bf.seq_id + 1) % max_size;
          tx_counter[t_i]++;
          bf.lock.unlock();
        } else if (FLAGS_type == "ticket") {
          u32 mask = 1;
          u64 ticket_no = aux.ticket.fetch_add(1, std::memory_order_release);
          u64 c_turn = bf.turn.load();
          while (c_turn != ticket_no) {
            const u64 sleep_time = (ticket_no > c_turn) ? ((ticket_no - c_turn) * FLAGS_backoff) : 0;
            for (u64 i = 0; i < sleep_time; i++)
              _mm_pause();
            c_turn = bf.turn.load();
          }
          {
            sequence[bf.seq_id] = t_i;
            bf.seq_id = (bf.seq_id + 1) % max_size;
            tx_counter[t_i]++;
            bf.turn.store(bf.turn.load() + 1, std::memory_order_release);
          }
        } else {
          jumpmuTry()
          {
            OptimisticGuard guard(bf.version);
            ExclusiveGuard ex_guard(guard);
            sequence[bf.seq_id] = t_i;
            bf.seq_id = (bf.seq_id + 1) % max_size;
            tx_counter[t_i]++;
          }
          jumpmuCatch() {}
        }
      }
    });
  }
  threads.emplace_back([&]() {
    while (keep_running) {
      u64 tx_sum = 0;
      for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++)
        tx_sum += tx_counter[t_i].exchange(0);
      cout << tx_sum / 1.0e6 << "\t" << sleep_counter.exchange(0) / 1.0e6 << endl;
      e.stopCounters();
      e.printReport(cout, tx_sum);
      e.startCounters();
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
    csv << "i,t,c_worker_threads,c_pin_threads,c_smt,type,c_backoff" << endl;
  }
  for (u64 i = 0; i < max_size; i++) {
    csv << i << "," << sequence[i] << "," << FLAGS_worker_threads << "," << FLAGS_pin_threads << "," << FLAGS_smt << "," << FLAGS_type << ","
        << FLAGS_x << endl;
  }
  return 0;
}
