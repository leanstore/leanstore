#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
class Parallelize
{
  public:
   static void range(u64 threads_count, u64 n, std::function<void(u64 t_i, u64 begin, u64 end)> callback);
   static void parallelRange(u64 n, std::function<void(u64, u64)>);
   // [begin, end]
   static void parallelRange(u64 begin, u64 end, u64 n_threads, std::function<void(u64)>);
};
}  // namespace utils
}  // namespace leanstore
