#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <arpa/inet.h>
#include <emmintrin.h>
#include <fcntl.h>
#include <linux/futex.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(threads, 8, "");
DEFINE_uint64(counters_per_cl, 1, "");
DEFINE_uint64(tmp, 10, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(pin, false, "");
DEFINE_bool(cl_exp, false, "");
DEFINE_bool(cond_futex, false, "");
DEFINE_bool(cond_std, false, "");
DEFINE_bool(exchange, false, "");
DEFINE_bool(release_cycles, false, "");
DEFINE_bool(mutex, false, "");
// -------------------------------------------------------------------------------------
struct alignas(64) CountersLine {
   std::atomic<u64> counter[8];
};
// -------------------------------------------------------------------------------------
/*
  CL Experiments:
  1 per cl : 1513293687
  2 per cl : 384459154
  4 per cl : 207514087
  8 per cl : 201057593
 */
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   vector<thread> threads;

   // -------------------------------------------------------------------------------------
   if (FLAGS_exchange) {
      CountersLine cl;
      CountersLine tl_counters;
      for (u64 t_i = 0; t_i < FLAGS_threads; t_i++) {
         threads.emplace_back([&, t_i]() {
            tl_counters.counter[t_i] = 0;
            while (true) {
               cl.counter[0].exchange(t_i);
               tl_counters.counter[t_i]++;
               // usleep(1);
            }
         });
      }
      threads.emplace_back([&]() {
         while (true) {
            u64 counter = 0;
            for (u64 t_i = 0; t_i < FLAGS_threads; t_i++) {
               counter += tl_counters.counter[t_i].exchange(0);
            }
            cout << counter << endl;
            sleep(1);
         }
      });
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_mutex) {
      std::mutex mutex;
      CountersLine tl_counters;
      for (u64 t_i = 0; t_i < FLAGS_threads; t_i++) {
         threads.emplace_back([&, t_i]() {
            tl_counters.counter[t_i] = 0;
            while (true) {
               mutex.lock();
               mutex.unlock();
               tl_counters.counter[t_i]++;
               // usleep(1);
            }
         });
      }
      threads.emplace_back([&]() {
         while (true) {
            u64 counter = 0;
            for (u64 t_i = 0; t_i < FLAGS_threads; t_i++) {
               counter += tl_counters.counter[t_i].exchange(0);
            }
            cout << counter << endl;
            sleep(1);
         }
      });
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_release_cycles) {
      atomic<bool> keep_running = true;
      struct alignas(64) CL {
         atomic<u64> counter = 0;
      };
      CL lock;
      threads.emplace_back([&]() {
         sleep(FLAGS_tmp);
         keep_running = false;
      });
      for (u64 t_i = 0; t_i < FLAGS_threads; t_i++) {
         threads.emplace_back([&, t_i]() {
            if (FLAGS_pin_threads)
               leanstore::utils::pinThisThreadRome(t_i);
            while (true) {
               u64 c_lock = lock.counter.load();
               DO_NOT_OPTIMIZE(c_lock);
            }
         });
      }
      threads.emplace_back([&]() {
         if (FLAGS_pin_threads)
            leanstore::utils::pinThisThreadRome(FLAGS_threads + 1);
         u64 counter = 0;
         PerfEvent e;
         e.startCounters();
         while (keep_running) {
            // lock.counter.store(counter++, std::memory_order_release);
            lock.counter.fetch_add(counter++, std::memory_order_seq_cst);
            // lock.counter.store(counter++, std::memory_order_seq_cst);
         }
         e.stopCounters();
         e.printReport(cout, FLAGS_tmp * counter);
      });
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_cond_futex) {
      CountersLine cl;
      std::atomic<int> f = 1;
      auto futex = [&](s32* uaddr, s32 futex_op, s32 val, const struct timespec* timeout, s32* uaddr2, s32 val3) {
         return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
      };
      // Use FUTEX_PRIVATE_FLAG as our futexes are process private.
      auto futex_wake = [&](s32* addr) { return futex(addr, FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0); };
      auto futex_wait = [&](s32* addr, s32 expected) {
         int futex_rc = futex(addr, FUTEX_WAIT_PRIVATE, expected, NULL, NULL, 0);
         if (futex_rc == 0) {
            return true;
         } else if (futex_rc == -1) {
            assert(errno == EAGAIN);
            return false;
         } else {
            throw;
         }
      };
      threads.emplace_back([&]() {
         while (true) {
            f.store(1, std::memory_order_release);
            futex_wait(reinterpret_cast<int*>(&f), 1);
            cl.counter[0]++;
         }
      });
      threads.emplace_back([&]() {
         while (true) {
            cout << cl.counter[0].exchange(0) << endl;
            sleep(1);
         }
      });
      while (true) {
         if (f.load(std::memory_order_acquire) == 1) {
            f.store(0, std::memory_order_release);
            futex_wake(reinterpret_cast<int*>(&f));
         }
      }
   }
   if (FLAGS_cond_std) {
      std::mutex m;
      std::condition_variable cv;
      CountersLine cl;

      threads.emplace_back([&]() {
         while (true) {
            // Wait until main() sends data
            std::unique_lock<std::mutex> lk(m);
            cv.wait(lk);

            // after the wait, we own the lock.
            // Manual unlocking is done before notifying, to avoid waking up
            // the waiting thread only to block again (see notify_one for details)
            lk.unlock();
            cl.counter[0]++;
         }
      });
      threads.emplace_back([&]() {
         while (true) {
            cout << cl.counter[0].exchange(0) << endl;
            sleep(1);
         }
      });
      while (true) {
         cv.notify_one();
      }
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_cl_exp) {
      auto pin = [&](int id) {
         if (!FLAGS_pin)
            return;
         cpu_set_t cpuset;
         CPU_ZERO(&cpuset);
         CPU_SET(id, &cpuset);

         pthread_t current_thread = pthread_self();
         if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0)
            throw;
      };

      /*

  1 131574494
  2 480121611
  3 697433641
  4 921887375
  10 2275469398 , 17x


        1 thread, 1 counters
        190083815
        32 threads, 32 counters
        8 counters pro cache line -> 503369534 , 2.64x to single thread
        4 counters pro cache line -> 791816677 , 1.5x to previous
        2 counters pro cache line -> 1589375893 , 2,0x to previous
        1 counters pro cache line -> 6091982294 , 3.8x to previous and 32.0x to single threaded

        64 threads, 1 : 12159421906
       */
      const u64 MAX_CL = 128;
      auto buffer = make_unique<u8[]>(64 * MAX_CL);
      auto local_counters = reinterpret_cast<std::atomic<u64>*>(buffer.get());
      // -------------------------------------------------------------------------------------
      if (FLAGS_threads > 1) {
         for (u64 t_i = 0; t_i < FLAGS_threads; t_i++) {
            const u64 cl_i = t_i / FLAGS_counters_per_cl;
            const u64 c_i = t_i % FLAGS_counters_per_cl;
            // cout << t_i << "," << cl_i << "," << c_i << endl;
            threads.emplace_back(
                [&](u64 cl_i, u64 i, u64 t_i) {
                   pin(t_i);
                   const u64 my_counter_i = (cl_i * 8) + i;
                   while (true) {
                      local_counters[my_counter_i]++;
                   }
                },
                cl_i, c_i, t_i);
         }
      } else {
         threads.emplace_back([&]() {
            u64 c_i = 0;
            pin(0);
            while (true) {
               const u64 my_counter_i = 0;
               local_counters[my_counter_i]++;
               //          local_counters[(c_i / 8) + ((c_i) % 8)]++;
               //          local_counters[c_i * 8]++;
               // c_i = (c_i + 1) % FLAGS_tmp;
            }
         });
      }
      threads.emplace_back([&]() {
         std::vector<u64> measurements;
         u64 i = 0;
         while (true) {
            u64 total = 0;
            for (u64 t_i = 0; t_i < 8 * MAX_CL; t_i++) {
               total += local_counters[t_i].exchange(0);
            }
            measurements.push_back(total);
            if (++i <= 6) {
               sleep(1);
            } else {
               // done
               auto top = std::max_element(measurements.begin() + 1, measurements.end());
               cout << FLAGS_threads << "," << FLAGS_counters_per_cl << "," << *top << endl;
               exit(0);
            }
         }
      });
   }
   for (auto& thread : threads) {
      thread.join();
   }
   return 0;
}
