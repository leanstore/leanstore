#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <emmintrin.h>
#include <errno.h>
#include <fcntl.h>
#include <libaio.h>
#include <linux/futex.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <ucontext.h>
#include <unistd.h>

#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
// -------------------------------------------------------------------------------------
constexpr u64 PAGE_SIZE = 16 * 1024;
constexpr u64 BUFFER_SIZE = 1024;
constexpr u64 STACK_SIZE = 16 * 1024 * 1024;
// -------------------------------------------------------------------------------------
DEFINE_bool(perf, false, "");
DEFINE_bool(libaio, false, "");
DEFINE_bool(liburing, false, "");
DEFINE_bool(userthreads, false, "");
DEFINE_bool(sync, false, "");
DEFINE_bool(interrupt, false, "");
DEFINE_uint64(range, 1024 * 1024 * 20, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
// https://www.bowdoin.edu/~sbarker/teaching/courses/os/18spring/p3.php
struct AIO {
   io_context_t aio_context;
   int fd, size;
   std::mutex mutex;
   std::vector<u64> free_slots;
   std::function<void()> read_commands[BUFFER_SIZE];
   struct iocb iocbs[BUFFER_SIZE];
   struct iocb* iocbs_ptr[BUFFER_SIZE];
   struct io_event events[BUFFER_SIZE];
   AIO(int fd) : fd(fd)
   {
      for (u64 i = 0; i < BUFFER_SIZE; i++)
         free_slots.push_back(i);
      // -------------------------------------------------------------------------------------
      memset(&aio_context, 0, sizeof(aio_context));
      const int ret = io_setup(BUFFER_SIZE, &aio_context);
      ensure(ret == 0);
   }
   void read(u64 pid, u8* destination, std::function<void()> callback)
   {
      while (free_slots.size() == 0) {
         poll();
      }
      auto slot = free_slots.back();
      free_slots.pop_back();
      io_prep_pread(&iocbs[slot], fd, destination, PAGE_SIZE, PAGE_SIZE * pid);
      iocbs_ptr[0] = &iocbs[slot];
      int ret_code = io_submit(aio_context, 1, iocbs_ptr);
      ensure(ret_code == 1);
      read_commands[slot] = callback;
      iocbs[slot].key = slot;
   }
   void readAsync(u64 pid, u8* destination, std::function<void()> before, std::function<void()> after)
   {
      while (free_slots.size() == 0) {
         poll();
      }
      auto slot = free_slots.back();
      free_slots.pop_back();
      io_prep_pread(&iocbs[slot], fd, destination, PAGE_SIZE, PAGE_SIZE * pid);
      iocbs_ptr[0] = &iocbs[slot];
      int ret_code = io_submit(aio_context, 1, iocbs_ptr);
      ensure(ret_code == 1);
      read_commands[slot] = after;
      iocbs[slot].key = slot;
      before();
   }
   int poll()
   {
      const u64 pending_requests = BUFFER_SIZE - free_slots.size();
      if (pending_requests > 0) {
         const int done_requests = io_getevents(aio_context, 0, BUFFER_SIZE, events, NULL);
         for (int i = 0; i < done_requests; i++) {
            const auto slot = events[i].obj->key;
            // -------------------------------------------------------------------------------------
            posix_check(events[i].res == PAGE_SIZE);
            explain(events[i].res2 == 0);
            read_commands[slot]();
            free_slots.push_back(slot);
         }
         return done_requests;
      }
      return 0;
   }
};
// -------------------------------------------------------------------------------------
struct alignas(512) Page {
   u8 data[PAGE_SIZE];
};
// -------------------------------------------------------------------------------------
struct UserThread {
   bool init = false;
   bool active = true;
   ucontext_t context;
   unique_ptr<u8[]> stack;
   std::function<void()> bootup, hello;
};
vector<UserThread> uts;
vector<u64> uts_active;
vector<u64> uts_blocked;
ucontext_t uctx_main;
static void exec(int t_i)
{
   uts[t_i].bootup();
   uts[t_i].active = false;
}
// -------------------------------------------------------------------------------------
ucontext_t uctx_perf;
static void perf(void)
{
   while (true)
      posix_check(swapcontext(&uctx_perf, &uctx_main) != -1);
}
// -------------------------------------------------------------------------------------
ucontext_t uctx_ping, uctx_pong;
atomic<bool> ping_or_pong = 0;
static void ping(void)
{
   while (true) {
      cout << "ping" << endl;
      posix_check(swapcontext(&uctx_ping, &uctx_main) != -1);
   }
}
static void pong(void)
{
   while (true) {
      cout << "pong" << endl;
      posix_check(swapcontext(&uctx_pong, &uctx_main) != -1);
   }
}
void sig_handler(int signo)
{
   bool ping = ping_or_pong;
   ping_or_pong = !ping_or_pong;
   if (ping) {
      posix_check(swapcontext(&uctx_main, &uctx_ping) != -1);
   } else {
      posix_check(swapcontext(&uctx_main, &uctx_pong) != -1);
   }
   return;
}
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   int flags = O_RDONLY | O_DIRECT;
   int fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   // -------------------------------------------------------------------------------------
   AIO aio(fd);
   // -------------------------------------------------------------------------------------
   // https://stackoverflow.com/questions/15651964/linux-can-a-signal-handler-excution-be-preempted
   if (FLAGS_interrupt) {
      atomic<s64> thread_id = -1;
      vector<thread> threads;
      threads.emplace_back([&]() {
         thread_id = syscall(__NR_gettid);
         if (signal(55, sig_handler) == SIG_ERR) {
            fputs("An error occurred while setting a signal handler.\n", stderr);
            exit(1);
         }

         u8* ping_stack = new u8[8 * 1024];
         posix_check(getcontext(&uctx_ping) != -1);
         uctx_ping.uc_stack.ss_sp = ping_stack;
         uctx_ping.uc_stack.ss_size = 8 * 1024;
         uctx_ping.uc_link = &uctx_main;
         makecontext(&uctx_ping, ping, 0);

         u8* pong_stack = new u8[8 * 1024];
         posix_check(getcontext(&uctx_pong) != -1);
         uctx_pong.uc_stack.ss_sp = pong_stack;
         uctx_pong.uc_stack.ss_size = 8 * 1024;
         uctx_pong.uc_link = &uctx_main;
         makecontext(&uctx_pong, pong, 0);
         while (true) {
            cout << "gonna sleep" << endl;
            sleep(10);
            cout << "while true" << endl;
         }
      });
      threads.emplace_back([&]() {
         while (true) {
            sleep(4);
            syscall(SYS_tkill, thread_id.load(), 55);
         }
      });
      for (auto& thread : threads) {
         thread.join();
      }
      cout << "no way" << endl;
   }
   if (FLAGS_perf) {
      {
         const u64 N = FLAGS_x;
         ucontext_t uctx_tmp;
         u8* stack = new u8[64];
         PerfEvent e;
         e.startCounters();
         for (u64 i = 0; i < N; i++) {
            posix_check(getcontext(&uctx_tmp) != -1);
            uctx_tmp.uc_stack.ss_sp = stack;
            uctx_tmp.uc_stack.ss_size = 64;
            uctx_tmp.uc_link = &uctx_main;
            makecontext(&uctx_tmp, perf, 0);
         }
         e.stopCounters();
         e.printReport(cout, N);
      }
      {
         const u64 N = FLAGS_x;
         u8* stack = new u8[8 * 1024];
         posix_check(getcontext(&uctx_perf) != -1);
         uctx_perf.uc_stack.ss_sp = stack;
         uctx_perf.uc_stack.ss_size = 8 * 1024;
         // uctx_perf.uc_link = NULL;
         uctx_perf.uc_link = &uctx_main;
         makecontext(&uctx_perf, perf, 0);
         PerfEvent e;
         e.startCounters();
         for (u64 i = 0; i < N; i++) {
            posix_check(swapcontext(&uctx_main, &uctx_perf) != -1);
         }
         e.stopCounters();
         e.printReport(cout, N * 2);
      }
   }
   if (FLAGS_userthreads) {
      // -------------------------------------------------------------------------------------
      for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         uts.push_back({});
         uts[t_i].stack = make_unique<u8[]>(STACK_SIZE);
         uts[t_i].bootup = [&, t_i]() {
            cout << "Hello user thread " << t_i << "," << uts[t_i].active << endl;
            auto page = make_unique<Page>();
            while (true) {
               aio.readAsync(
                   utils::RandomGenerator::getRand<u64>(0, FLAGS_range), page->data,
                   [t_i]() {
                      uts[t_i].active = false;
                      posix_check(swapcontext(&uts[t_i].context, &uctx_main) != -1);
                   },
                   [t_i]() { uts[t_i].active = true; });
            }
         };
         uts_active.push_back(t_i);
      }
      // -------------------------------------------------------------------------------------
      vector<thread> threads;
      threads.emplace_back([&]() {
         utils::pinThisThreadRome(1);
         while (true) {
            for (volatile u64 t_i = 0; t_i < uts.size(); t_i++) {
               auto& th = uts[t_i];
               if (!th.active) {
                  continue;
               }
               if (!th.init) {
                  posix_check(getcontext(&th.context) != -1);
                  th.context.uc_stack.ss_sp = th.stack.get();
                  th.context.uc_stack.ss_size = STACK_SIZE;
                  th.context.uc_link = &uctx_main;
                  th.init = true;
                  makecontext(&th.context, (void (*)())exec, 1, t_i);
               }
               posix_check(swapcontext(&uctx_main, &th.context) != -1);
            }
            aio.poll();
         }
      });
      for (auto& thread : threads) {
         thread.join();
      }
      cout << "no way " << endl;
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_sync) {
      vector<thread> threads;
      for (u64 i = 0; i < FLAGS_worker_threads; i++) {
         threads.emplace_back([&]() {
            auto page = make_unique<Page>();
            while (true) {
               pread(fd, page->data, PAGE_SIZE, utils::RandomGenerator::getRand<u64>(0, FLAGS_range) * PAGE_SIZE);
            }
         });
      }
      for (auto& thread : threads) {
         thread.join();
      }
   }
   // -------------------------------------------------------------------------------------
   return 0;
}
