#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include<functional>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
class Parallelize
{
 public:
  static void parallelRange(u64 n, std::function<void(u64, u64)>);
};
}
}
