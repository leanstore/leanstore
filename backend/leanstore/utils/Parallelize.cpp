#include "Parallelize.hpp"

#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
void Parallelize::parallelRange(u64 n, std::function<void(u64 begin, u64 end)> callback)
{
  const u64 hw_threads = std::thread::hardware_concurrency();
  std::vector<std::thread> threads;
  const u64 block_size = n / hw_threads;
  ensure(block_size > 0);
  for (u64 t_i = 0; t_i < hw_threads; t_i++) {
    u64 begin = (t_i * block_size);
    u64 end = begin + (block_size);
    if (t_i == hw_threads - 1) {
      end = n;
    }
    threads.emplace_back([&](u64 begin, u64 end) { callback(begin, end); }, begin, end);
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
// -------------------------------------------------------------------------------------
void Parallelize::parallelRange(u64 a, u64 b, u64 n_threads, std::function<void(u64 begin, u64 end)> callback)
{
  u64 n = b - a + 1;
  std::vector<std::thread> threads;
  const u64 block_size = n / n_threads;
  ensure(block_size > 0);
  for (u64 t_i = 0; t_i < n_threads; t_i++) {
    u64 begin = a + (t_i * block_size);
    u64 end = begin + (block_size);
    if (t_i == n_threads - 1) {
      end = b;
    }
    threads.emplace_back([&callback](u64 begin, u64 end) { callback(begin, end); }, begin, end);
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
