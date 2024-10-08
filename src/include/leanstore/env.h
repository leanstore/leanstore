#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "common/worker_pool.h"

#include <array>
#include <atomic>

namespace leanstore {

namespace buffer {
class BufferManager;
}

// Environment vars
extern std::vector<buffer::BufferManager *> all_buffer_pools;
extern std::atomic<bool> start_profiling;
extern std::atomic<bool> start_profiling_latency;
extern thread_local wid_t worker_thread_id;

}  // namespace leanstore
