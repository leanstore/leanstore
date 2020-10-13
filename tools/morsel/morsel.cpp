#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <emmintrin.h>
#include <fcntl.h>
#include <linux/futex.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(worker_threads, 6, "");
// -------------------------------------------------------------------------------------
struct alignas(64) WorkerThread {
   int futex;
   struct Queue {
      constexpr static u64 QUEUE_SIZE = 10;
      atomic<std::function<void()>*> f[QUEUE_SIZE] = {nullptr};
      atomic<u64> head = 0;  // done
      atomic<u64> tail = 0;  // submitted
      u64 myTail() { return tail % QUEUE_SIZE; }
      void push(std::function<void()>* func)
      {
         ensure((tail % QUEUE_SIZE) != head - 1);
         f[tail] = func;
         tail++;
      }
   };
   Queue oltp, olap;
};
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Morsel driven Framework");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   WorkerThread worker_threads[FLAGS_worker_threads];
   std::vector<thread> threads;
   atomic<bool> keep_running = true;
   atomic<u64> running_threads = 0;
   // -------------------------------------------------------------------------------------
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back([&, t_i] {
         running_threads++;
         auto& meta = worker_threads[t_i];
         while (keep_running) {
            auto process_queue = [&](WorkerThread::Queue& queue) {
               if (queue.head != queue.tail) {
                  ensure(queue.f[queue.head] != nullptr);
                  (*queue.f[queue.head])();
                  queue.head++;
                  return true;
               }
               return false;
            };
            if (!process_queue(meta.oltp) && !process_queue(meta.olap))
               usleep(1);
         }
         running_threads--;
         return;
      });
   }
   // -------------------------------------------------------------------------------------
   std::function<void()> test([&]() { cout << "Hello World " << endl; });
   auto& q = worker_threads[0].oltp;

   // -------------------------------------------------------------------------------------
   sleep(5);
   keep_running = false;
   while (running_threads) {
      _mm_pause();
   }
   for (auto& thread : threads) {
      thread.join();
   }
   // -------------------------------------------------------------------------------------
   return 0;
}
