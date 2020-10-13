
#include "Exceptions.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/sync-primitives/PlainGuard.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/futex.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(sleep_us, 1, "");
DEFINE_uint64(operations, 1, "");
DEFINE_bool(affinity, false, "");
DEFINE_bool(split, false, "");
DEFINE_bool(pin, false, "");
DEFINE_uint64(waste, 1e2, "");
DEFINE_uint64(work, 1e2, "");
// -------------------------------------------------------------------------------------
void tx() {}
// -------------------------------------------------------------------------------------
/*
  EPYC Rome
  L1: 4 cycles
  L2: 13 cycles
  LLC: 34
 */
// -------------------------------------------------------------------------------------
using namespace std;
using leanstore::utils::RandomGenerator;
using namespace leanstore::buffermanager;

struct alignas(64) BF {
   HybridLatch latch;
};
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ensure(FLAGS_worker_threads > 0);
   vector<thread> threads;
   std::ofstream csv;
   csv.open(FLAGS_csv_path.c_str(), ios::trunc);
   csv << std::setprecision(2) << std::fixed;
   {
      BF bfs[100 * FLAGS_worker_threads];
      atomic<u64> tx_counter[FLAGS_worker_threads] = {0};
      std::array<u8, 128> payload = {0};
      std::array<u8, 128> dump = {1};
      // memcpy 128 bytes: 9 cycles, 12 instructions

      if (0) {
         PerfEvent e;
         PerfEventBlock b(e, 1e6);
         for (u64 i = 0; i < 1e6; i++)
            std::memcpy(dump.data(), payload.data(), 128);
      }
      auto ex_lock = [&](BF& bf) {
         atomic<u64> waste_cycles = 0;
         OptimisticGuard guard(bf.latch);
         for (u64 i = 0; i < FLAGS_waste; i++)
            waste_cycles += i + 1;
         DO_NOT_OPTIMIZE(waste_cycles);
         ExclusiveGuard x_guard(guard);
         DO_NOT_OPTIMIZE(payload.data());
         DO_NOT_OPTIMIZE(dump.data());
         std::memcpy(dump.data(), payload.data(), 128);
         // for (u64 i = 0; i < 128; i++)
         //    dump[i] = payload[i];
         // waste_cycles += dump[0];
         DO_NOT_OPTIMIZE(payload.data());
         DO_NOT_OPTIMIZE(dump.data());
      };
#define EASY +((FLAGS_affinity) ? t_i : RandomGenerator::getRandU64(0, FLAGS_worker_threads))
#define HARD +((FLAGS_split) ? t_i : 0)
      auto tx = [&](u64 t_i) {
         int rnd = leanstore::utils::RandomGenerator::getRand(0, 1000);
         if (rnd < 430) {
            ex_lock(bfs[(0 * FLAGS_worker_threads) HARD]);
            return;
         }
         rnd -= 430;
         if (rnd < 40) {
            ex_lock(bfs[(2 * FLAGS_worker_threads) EASY]);
            return;
         }
         rnd -= 40;
         if (rnd < 40) {
            ex_lock(bfs[(4 * FLAGS_worker_threads) EASY]);
            return;
         }
         rnd -= 40;
         if (rnd < 40) {
            ex_lock(bfs[(6 * FLAGS_worker_threads) EASY]);
            return;
         }
         rnd -= 40;
         ex_lock(bfs[(8 * FLAGS_worker_threads) EASY]);
      };
      for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++)
         threads.emplace_back(
             [&](int t_i) {
                if (FLAGS_pin)
                   leanstore::utils::pinThisThreadRome();
                while (true) {
                   jumpmuTry()
                   {
                      ex_lock(bfs[0]);
                      //                tx(t_i);
                      tx_counter[t_i]++;
                   }
                   jumpmuCatch() {}
                }
             },
             t_i);
      // -------------------------------------------------------------------------------------
      sleep(1);
      threads.emplace_back([&]() {
         u64 t = 0;
         u64 tx_sum = 0;
         for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++)
            tx_sum += tx_counter[t_i].exchange(0);
         while (true) {
            cout << t << "," << tx_sum / 1.0e6 << "\n";
            csv << t << "," << tx_sum / 1.0e6 << "\n";
            t++;
            sleep(1);
         }
      });
      for (auto& thread : threads) {
         thread.join();
      }
   }
   // -------------------------------------------------------------------------------------
   return 0;
}
