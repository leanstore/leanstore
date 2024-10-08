#include "leanstore/env.h"
#include "leanstore/config.h"

#include <atomic>
#include <cassert>
#include <deque>
#include <unordered_set>

namespace leanstore {

// All env variables
std::vector<buffer::BufferManager *> all_buffer_pools;
std::atomic<bool> start_profiling         = false;  // Whether the profiling thread is running
std::atomic<bool> start_profiling_latency = false;  // Whether the profiling thread is running
thread_local wid_t worker_thread_id       = -1;     // The ID of current worker (min wid is 0)

}  // namespace leanstore
