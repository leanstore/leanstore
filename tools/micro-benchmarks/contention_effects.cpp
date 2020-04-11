#include "Exceptions.hpp"
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

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(threads, 8, "");
DEFINE_uint64(counters_per_cl, 1, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(cl_exp, false, "");
DEFINE_bool(cond_futex, false, "");
DEFINE_bool(cond_std, false, "");
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
    auto buffer = make_unique<u8[]>(64 * 8);
    auto local_counters = reinterpret_cast<std::atomic<u64>*>(buffer.get());
    // -------------------------------------------------------------------------------------
    for (u64 t_i = 0; t_i < FLAGS_threads; t_i++) {
      threads.emplace_back(
          [&](u64 cl_i, u64 i) {
            cout << cl_i << "," << i << endl;
            while (true) {
              local_counters[(cl_i * 8) + i]++;
            }
          },
          t_i / FLAGS_counters_per_cl, t_i % FLAGS_counters_per_cl);
    }
    threads.emplace_back([&]() {
      while (true) {
        u64 total = 0;
        for (u64 t_i = 0; t_i < FLAGS_threads * 8; t_i++) {
          total += local_counters[t_i].exchange(0);
        }
        cout << total << endl;
        sleep(1);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  return 0;
}
