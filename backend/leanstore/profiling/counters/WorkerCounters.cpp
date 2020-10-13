#include "WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
atomic<u64> WorkerCounters::workers_counter = 0;
tbb::enumerable_thread_specific<WorkerCounters> WorkerCounters::worker_counters;
}  // namespace leanstore
