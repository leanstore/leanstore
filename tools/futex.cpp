#include "Exceptions.hpp"
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
#include <shared_mutex>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(worker_threads, 20, "");
DEFINE_uint64(groups_count, 1, "");
DEFINE_uint64(samples_count, (1 << 10), "");
DEFINE_bool(futex, false, "");
DEFINE_bool(mutex, false, "");
DEFINE_bool(cmpxchg, false, "");
DEFINE_bool(seq, false, "");
// -------------------------------------------------------------------------------------
/*
struct mutex {
int val = 0;
void lock() {
int c;
if ((c= cmpxchg(val, 0, 1)) != 0 ) {
do {
if(c ==2 || cmpxchg(val, 1, 2) != 0) {
futex_wait(&val, 2);
}
} while((c = cmpxchg(val, 0, 2)) != 0);
}
}
void unlock() {
     if(atomic_dec(val) != 1) {
        val = 0;
        futex_wake(&val, 1);
        }
}
};

What happens if we set W but don't sleep ? won't affect correctness but just performance because we will call futex_wake although no one is waiting
When you wake up from futex_wait, you have to set the W flag during locking
  Latch format:
L: SHARED_COUNTERS | VERSION | X (EXCLUSIVELY_LATCHED) | S (SHARED_LATCHED | W (SOMEONE_WAITING)

optimistic latch: load L, if (X is set), spin for a while (optional), then set W flag && sleep futex_wait(addr, upper 32-bits);
upgrade to exclusive latch: try CAS, failed: is it now exclusively locked ? spin and try again/sleep, is it a different version, then simply restart.
recheck() : load L, mask for version


Unlocking: it seems that we have no escape from CAS and goto.

 */
constexpr u64 WAITING_FLAG = 1 << 0;
constexpr u64 SHARED_FLAG = 1 << 1;
constexpr u64 EXCLUSIVE_FLAG = 1 << 2;
constexpr u64 SHARED_COUNTER_MASK = (0xFFul << 56);
constexpr u64 VERSION_MASK = ~(SHARED_COUNTER_MASK | SHARED_FLAG | WAITING_FLAG);
s32 futex(s32* uaddr, s32 futex_op, s32 val, const struct timespec* timeout, s32* uaddr2, s32 val3)
{
   return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}
// Use FUTEX_PRIVATE_FLAG as our futexes are process private.
s32 futex_wake(s32* addr)
{
   return futex(addr, FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0);
}
bool futex_wait(s32* addr, s32 expected)
{
   int futex_rc = futex(addr, FUTEX_WAIT_PRIVATE, expected, NULL, NULL, 0);
   if (futex_rc == 0) {
      return true;
   } else if (futex_rc == -1) {
      assert(errno == EAGAIN);
      return false;
   } else {
      throw;
   }
}
void futex_lock(s32* addr) {}
void exclusive_lock(atomic<u64>* latch)
{
   // 1- Fast Path:
   u64 c = latch->load();
   if ((c & (EXCLUSIVE_FLAG | SHARED_FLAG)) == 0) {
      u64 e = c | EXCLUSIVE_FLAG;
      if (latch->compare_exchange_strong(c, e)) {
      } else {
      }
   } else {
   }
}
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
   std::shared_mutex at;
   cout << sizeof(at) << endl;
   // -------------------------------------------------------------------------------------
   gflags::SetUsageMessage("");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ensure(FLAGS_worker_threads > 0);
   atomic<u64> latches[FLAGS_worker_threads] = {0};
   // printf("%p\n", WAITING_FLAG);
   // printf("%p\n", SHARED_FLAG);
   // printf("%p\n", EXCLUSIVE_FLAG);
   // printf("%p\n", SHARED_COUNTER_MASK);
   // printf("%p\n", VERSION_MASK);
   // -------------------------------------------------------------------------------------
   vector<thread> threads;
   atomic<bool> keep_running = true;
   alignas(64) atomic<u64> tx_counter[FLAGS_worker_threads] = {0};
   {
      const u64 group_size = FLAGS_worker_threads / FLAGS_groups_count;
      // -------------------------------------------------------------------------------------
      atomic<s32> locks[FLAGS_groups_count] = {0};
      std::mutex mlocks[FLAGS_groups_count];
      // -------------------------------------------------------------------------------------
      atomic<u64> counter = 0;
      atomic<u64> sleep_counter = 0;
      std::array<u8, 128> payload = {0};
      std::array<u8, 128> dump = {1};
      // -------------------------------------------------------------------------------------
      if (FLAGS_seq) {
         struct BF {
            mutex lock;
            atomic<u64> version = 0;
            u64 seq_id = 0;
         };
         BF bf;
         const u64 max_size = FLAGS_samples_count;
         s32* sequence = new s32[max_size];
         for (s32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
            threads.emplace_back(
                [&](int t_i) {
                   while (keep_running) {
                      if (FLAGS_mutex) {
                         bf.lock.lock();
                         sequence[bf.seq_id] = t_i;
                         bf.seq_id = (bf.seq_id + 1) % max_size;
                         tx_counter[t_i]++;
                         bf.lock.unlock();
                      } else {
                         u64 e = bf.version.load();
                         while (e & 1) {
                            _mm_pause();
                            e = bf.version.load();
                         }
                         u64 c = e | 1;
                         if (bf.version.compare_exchange_strong(e, c)) {
                            sequence[bf.seq_id] = t_i;
                            bf.seq_id = (bf.seq_id + 1) % max_size;
                            tx_counter[t_i]++;
                            bf.version--;
                         }
                      }
                   }
                },
                t_i);
         }
         sleep(4);
         keep_running = false;
         cout << "shutting down" << endl;
         for (auto& thread : threads) {
            thread.join();
         }
         threads.clear();
         cout << "threads down" << endl;
         std::ofstream csv;
         csv.open("seq.csv", ios::trunc);
         csv << std::setprecision(2) << std::fixed << "i,t" << endl;
         for (u64 i = 0; i < max_size; i++) {
            csv << "i," << sequence[i] << endl;
         }
         return 0;
      } else {
         for (u64 g_i = 0; g_i < FLAGS_groups_count; g_i++) {
            for (u64 t_i = g_i * group_size; t_i < (g_i + 1) * group_size; t_i++)
               threads.emplace_back(
                   [&](int g_i, int t_i) {
                      auto& lock = locks[g_i];
                      auto& mlock = mlocks[g_i];
                      while (true) {
                         if (FLAGS_mutex) {
                            mlock.lock();
                            std::memcpy(dump.data(), payload.data(), 128);
                            tx_counter[t_i]++;
                            mlock.unlock();
                         } else if (FLAGS_cmpxchg) {
                            s32 e = 0;
                            while (!lock.compare_exchange_strong(e, 1)) {
                               e = 0;
                            }
                            std::memcpy(dump.data(), payload.data(), 128);
                            tx_counter[t_i]++;
                            lock--;
                         } else if (FLAGS_futex) {
                            s32 e = lock.load();
                            while (e & 1) {
                               if (FLAGS_futex && futex_wait(reinterpret_cast<s32*>(&lock), e)) {
                                  sleep_counter++;
                               }
                               e = lock.load();
                            }
                            s32 c = e | 1;
                            if (lock.compare_exchange_strong(e, c)) {
                               std::memcpy(dump.data(), payload.data(), 128);
                               tx_counter[t_i]++;
                               lock--;
                               futex_wake(reinterpret_cast<s32*>(&lock));
                            }
                         } else {
                            s32 e = lock.load();
                            while (e & 1) {
                               _mm_pause();
                               e = lock.load();
                            }
                            s32 c = e | 1;
                            if (lock.compare_exchange_strong(e, c)) {
                               std::memcpy(dump.data(), payload.data(), 128);
                               tx_counter[t_i]++;
                               lock--;
                            }
                         }
                      }
                   },
                   g_i, t_i);
         }
      }
      // -------------------------------------------------------------------------------------
      threads.emplace_back([&]() {
         while (true) {
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
   }
   return 0;
}
