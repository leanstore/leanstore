#include "Exceptions.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/LeanStore.hpp"
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
DEFINE_uint64(tmp, 16 * 1024, "");
DEFINE_uint64(extra, 0, "");
DEFINE_uint64(cl, 1, "");
DEFINE_bool(tls, false, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
using namespace leanstore::buffermanager;
struct alignas(64) CL {
  atomic<u64> raw = 0;
};
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  const u64 max_size = FLAGS_samples_count;
  u64* sequence = new u64[max_size];
  CL seq_cur;
  // -------------------------------------------------------------------------------------
  ensure(FLAGS_worker_threads > 0);
  vector<thread> threads;
  atomic<bool> keep_running = true;
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  for (u64 cl_i = 0; cl_i < FLAGS_cl; cl_i++) {
    keep_running = true;
    CL contended[1024];
    atomic<u64> threads_counter = 0;
    u64 page[FLAGS_tmp];
    std::mutex io_mutex;
    for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back([&, t_i]() {
        threads_counter++;
        utils::pinThisThreadRome(FLAGS_pp_threads + t_i);
        u64 tls_page[FLAGS_tmp];
        while (keep_running) {
          u64 v = contended[cl_i].raw.load();
          u64 tmp = 0;
          if (FLAGS_tls) {
            for (u64 i = 0; i < FLAGS_tmp; i++)
              tmp += tls_page[i];
          } else {
            for (u64 i = 0; i < FLAGS_tmp; i++)
              tmp += page[i];
          }
          DO_NOT_OPTIMIZE(tmp);
          if (!contended[cl_i].raw.compare_exchange_strong(v, v + 1)) {
            // if (FLAGS_tls) {
            //   for (u64 i = 0; i < FLAGS_tmp; i++)
            //     tmp += tls_page[i];
            // } else {
            //   for (u64 i = 0; i < FLAGS_tmp; i++)
            //     tmp += page[i];
            // }
            continue;
          }
          sequence[seq_cur.raw++ % max_size] = t_i;
        }
        threads_counter--;
      });
    }
    sleep(FLAGS_run_for_seconds);
    keep_running = false;
    while (threads_counter) {
    }
    for (auto& thread : threads) {
      thread.join();
    }
    threads.clear();
    // -------------------------------------------------------------------------------------
    std::map<u64, u64> agg;
    for (u64 i = 0; i < FLAGS_worker_threads; i++) {
      agg[i] = 0;
    }
    for (u64 i = 0; i < max_size; i++) {
      agg[sequence[i]]++;
    }
    cout << "t,freq" << endl;
    for (u64 i = 0; i < FLAGS_worker_threads; i++) {
      cout << i << "," << agg[i] << endl;
    }
  }
  // -------------------------------------------------------------------------------------
  return 0;
}
