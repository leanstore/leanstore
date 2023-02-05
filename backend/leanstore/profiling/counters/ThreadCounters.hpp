#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/enumerable_thread_specific.h>

#include "leanstore/concurrency/Mean.hpp"
#include "leanstore/utils/Hist.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
struct ThreadCounters {
   atomic<u64> t_id = mean::exec::getId();
   // -------------------------------------------------------------------------------------
   atomic<u64> tx = 0; 
   atomic<u64> tx_abort = 0; 
   atomic<u64> exec_cycles = 0; 
   atomic<u64> exec_cycl_max_dur = 0; 
   atomic<u64> exec_cycl_max_poll_us= 0; 
   atomic<u64> exec_cycl_max_subm_us= 0; 
   atomic<u64> exec_cycl_max_task_us= 0; 
   atomic<u64> exec_tasks_run = 0; 
   atomic<u64> exec_no_tasks_run = 0; 
   atomic<u64> exec_tasks_st_comp = 0; 
   atomic<u64> exec_tasks_st_wait = 0; 
   atomic<u64> exec_tasks_st_wait_io = 0; 
   atomic<u64> exec_tasks_st_ready_mem = 0; 
   atomic<u64> exec_tasks_st_ready_lck = 0; 
   atomic<u64> exec_tasks_st_ready_lckskip = 0; 
   atomic<u64> exec_tasks_st_ready_jumplck = 0; 
   atomic<u64> exec_tasks_st_ready = 0; 
   atomic<u64> pp_p1_picked = 0; 
   atomic<u64> pp_p23_evicted = 0; 
   atomic<u64> pp_p2_iopushed = 0; 
   // -------------------------------------------------------------------------------------
   ThreadCounters() { }
   // -------------------------------------------------------------------------------------
   static tbb::enumerable_thread_specific<ThreadCounters> thread_counters;
   static tbb::enumerable_thread_specific<ThreadCounters>::reference myCounters() { return thread_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------
