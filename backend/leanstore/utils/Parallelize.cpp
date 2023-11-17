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
// -------------------------------------------------------------------------------------
void Parallelize::range(u64 threads_count, u64 n, std::function<void(u64 t_i, u64 begin, u64 end)> callback)
{
   const u64 block_size = n / threads_count;
   ensure(block_size > 0);
   for (u64 t_i = 0; t_i < threads_count; t_i++) {
      u64 begin = (t_i * block_size);
      u64 end = begin + (block_size);
      if (t_i == threads_count - 1) {
         end = n;
      }
      callback(t_i, begin, end);
   }
}
// -------------------------------------------------------------------------------------
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
