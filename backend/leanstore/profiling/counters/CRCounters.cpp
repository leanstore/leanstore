#include "CRCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
atomic<u64> CRCounters::cr_partitions_counters = 0;
tbb::enumerable_thread_specific<CRCounters> CRCounters::cr_counters;
}
