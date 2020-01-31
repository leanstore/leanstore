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
template <class CountersClass, class CounterType>
u64 sum(tbb::enumerable_thread_specific<CountersClass>& counters, CounterType CountersClass::*c)
{
  u64 local_c = 0;
  for (typename tbb::enumerable_thread_specific<CountersClass>::iterator i = counters.begin(); i != counters.end(); ++i) {
    local_c += ((*i).*c).exchange(0);
  }
  return local_c;
}
// -------------------------------------------------------------------------------------
template <class CountersClass, class CounterType>
u64 sum(tbb::enumerable_thread_specific<CountersClass>& counters, CounterType CountersClass::*c, u8 index)
{
  u64 local_c = 0;
  for (typename tbb::enumerable_thread_specific<CountersClass>::iterator i = counters.begin(); i != counters.end(); ++i) {
    local_c += ((*i).*c)[index].exchange(0);
  }
  return local_c;
}
// -------------------------------------------------------------------------------------
template <class CountersClass, class CounterType>
u64 sum(tbb::enumerable_thread_specific<CountersClass>& counters, CounterType CountersClass::*c, u8 row, u8 col)
{
  u64 local_c = 0;
  for (typename tbb::enumerable_thread_specific<CountersClass>::iterator i = counters.begin(); i != counters.end(); ++i) {
    local_c += ((*i).*c)[row][col].exchange(0);
  }
  return local_c;
}
// -------------------------------------------------------------------------------------
}  // namespace threadlocal
}  // namespace utils
}  // namespace leanstore
