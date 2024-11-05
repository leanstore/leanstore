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
   const u64 block_size = std::ceil(n * 1.0 / threads_count);
   for(u64 t_i = 0; t_i < threads_count-1; t_i++){
      callback(t_i, block_size*t_i, block_size*(t_i+1));
   }
   callback(threads_count-1, block_size*(threads_count-1), n);
}
// -------------------------------------------------------------------------------------
void Parallelize::parallelRange(u64 n, std::function<void(u64 begin, u64 end)> callback)
{
   const u64 hw_threads = std::thread::hardware_concurrency();
   std::vector<std::thread> threads;
   range(hw_threads, n, [&](u64 t_i, u64 begin, u64 end) {
      threads.emplace_back([&](u64 inner_begin, u64 inner_end) { callback(inner_begin, inner_end); }, begin, end);
   });
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
