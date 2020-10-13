#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/futex.h>
#include <pthread.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(worker_threads, 20, "");
// -------------------------------------------------------------------------------------
atomic<u64> counter = 0;
void sig_handler(int signo)
{
   return;
}
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   {
      PerfEvent e;
      e.setParam("op", "getpid");
      const u64 n = 1e6;
      PerfEventBlock b(e, n);
      for (u64 i = 0; i < n; i++) {
         syscall(SYS_getuid);
      }
   }
   // -------------------------------------------------------------------------------------
   vector<thread> threads;
   // -------------------------------------------------------------------------------------
   atomic<s64> thread_id = -1;
   threads.emplace_back([&]() {
      thread_id = syscall(__NR_gettid);
      if (signal(55, sig_handler) == SIG_ERR) {
         fputs("An error occurred while setting a signal handler.\n", stderr);
         return EXIT_FAILURE;
      }
      while (true) {
         int ret = sleep(1);
         if (ret != 0) {
            cout << ret << endl;
         }
         counter++;
      }
   });
   while (thread_id == -1) {
   }
   {
      PerfEvent e;
      const u64 n = 1e6;
      PerfEventBlock b(e, n);
      for (u64 i = 0; i < n; i++) {
         syscall(SYS_tkill, thread_id.load(), 55);
      }
   }
   cout << thread_id << endl;
   threads.emplace_back([&]() {
      while (true) {
         if (thread_id != -1)
            syscall(SYS_tkill, thread_id.load(), 55);
         sleep(2);
      }
   });
   threads.emplace_back([&]() {
      while (true) {
         cout << counter.exchange(0) << endl;
         sleep(1);
      }
   });
   for (auto& thread : threads) {
      thread.join();
   }
   return 0;
}
