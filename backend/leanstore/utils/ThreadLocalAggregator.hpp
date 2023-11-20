#pragma once
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <tbb/enumerable_thread_specific.h>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
namespace threadlocal
{
template <class CountersClass, class CounterType, typename T = u64>
T sum_reset(tbb::enumerable_thread_specific<CountersClass>& counters, CounterType CountersClass::*c)
{
   T local_c = 0;
   for (typename tbb::enumerable_thread_specific<CountersClass>::iterator i = counters.begin(); i != counters.end(); ++i) {
      local_c += ((*i).*c).exchange(0);
   }
   return local_c;
}
// -------------------------------------------------------------------------------------
template <class CountersClass, class CounterType, typename T = u64>
T sum_reset(tbb::enumerable_thread_specific<CountersClass>& counters, CounterType CountersClass::*c, u64 index)
{
   T local_c = 0;
   for (typename tbb::enumerable_thread_specific<CountersClass>::iterator i = counters.begin(); i != counters.end(); ++i) {
      local_c += ((*i).*c)[index].exchange(0);
   }
   return local_c;
}
// -------------------------------------------------------------------------------------
template <class CountersClass, class CounterType, typename T = u64>
T sum_reset(tbb::enumerable_thread_specific<CountersClass>& counters, CounterType CountersClass::*c, u64 row, u64 col)
{
   T local_c = 0;
   for (typename tbb::enumerable_thread_specific<CountersClass>::iterator i = counters.begin(); i != counters.end(); ++i) {
      local_c += ((*i).*c)[row][col].exchange(0);
   }
   return local_c;
}
// -------------------------------------------------------------------------------------
template <class CountersClass, class CounterType, typename T = u64>
T max_reset(tbb::enumerable_thread_specific<CountersClass>& counters, CounterType CountersClass::*c, u64 row)
{
   T local_c = 0;
   for (typename tbb::enumerable_thread_specific<CountersClass>::iterator i = counters.begin(); i != counters.end(); ++i) {
      local_c = std::max<T>(((*i).*c)[row].exchange(0), local_c);
   }
   return local_c;
}
// -------------------------------------------------------------------------------------
}  // namespace threadlocal
}  // namespace utils
}  // namespace leanstore
