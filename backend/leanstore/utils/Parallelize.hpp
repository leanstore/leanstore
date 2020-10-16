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
   static void parallelRange(u64 n, std::function<void(u64, u64)>);
   // [begin, end]
   static void parallelRange(u64 begin, u64 end, u64 n_threads, std::function<void(u64)>);
};
}  // namespace utils
}  // namespace leanstore
