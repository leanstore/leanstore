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
CounterType sum(tbb::enumerable_thread_specific<CountersClass>& counters, CounterType CountersClass::*c)
{
  CounterType local_c = 0;
  for (typename tbb::enumerable_thread_specific<CountersClass>::iterator i = counters.begin(); i != counters.end(); ++i) {
    local_c += (*i).*c;
    (*i).*c = 0;
  }
  return local_c;
}
// -------------------------------------------------------------------------------------
}  // namespace threadlocal
}  // namespace utils
}  // namespace leanstore
