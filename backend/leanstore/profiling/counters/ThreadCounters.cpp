#include "ThreadCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
tbb::enumerable_thread_specific<ThreadCounters> ThreadCounters::thread_counters;
}  // namespace leanstore
