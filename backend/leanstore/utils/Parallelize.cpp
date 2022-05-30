#include "Parallelize.hpp"

#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
void Parallelize::range(u64 threads_count, u64 n, std::function<void(u64 t_i, u64 begin, u64 end)> callback)
{
   const u64 block_size = std::ceil(n * 1.0 / threads_count);
   u64 start = 0;
   u64 t_i =0;
   while(start < n){
      u64 end = start + block_size;
      if (end >= n){
         end = n;
      }
      callback(t_i, start, end);
      start = end;
      t_i ++;
   }
}

void Parallelize::parallelRange(u64 n, std::function<void(u64 begin, u64 end)> callback)
{
   const u64 hw_threads = std::thread::hardware_concurrency();
   std::vector<std::thread> threads;
   const u64 block_size = std::ceil(n * 1.0 / hw_threads);
   u64 start = 0;
   while(start < n){
      u64 end = start + block_size;
      if (end >= n){
         end = n;
      }
      threads.emplace_back([&](u64 begin, u64 end) { callback(begin, end); }, start, end);
      start = end;
   }
   for (auto& thread : threads) {
      thread.join();
   }
}
// -------------------------------------------------------------------------------------
void Parallelize::parallelRange(u64 a, u64 b, u64 n_threads, std::function<void(u64 i)> callback)
{
   std::vector<std::thread> threads;
   // -------------------------------------------------------------------------------------
   std::mutex m;
   std::condition_variable cv;
   u64 active_threads = 0;
   // -------------------------------------------------------------------------------------
   while (a <= b) {
      std::unique_lock<std::mutex> lk(m);
      cv.wait(lk, [&] { return active_threads < n_threads; });
      active_threads++;
      threads.emplace_back(
          [&](u64 i) {
             callback(i);
             {
                std::unique_lock<std::mutex> lk(m);
                active_threads--;
             }
             cv.notify_all();
          },
          a++);
   }
   for (auto& thread : threads) {
      thread.join();
   }
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
