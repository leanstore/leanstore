#include "ThreadTable.hpp"
#include <emmintrin.h>

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/ThreadCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
#include "leanstore/concurrency/Mean.hpp"
// -------------------------------------------------------------------------------------
#include <iomanip>
#include <string>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
ThreadTable::ThreadTable() : ProfilingTable() {}
// -------------------------------------------------------------------------------------
std::string ThreadTable::getName()
{
   return "thr";
}
// -------------------------------------------------------------------------------------
template<typename T>
T loadResetValue(std::atomic<T>& val) {
   return val.exchange(0);
}
// -------------------------------------------------------------------------------------
void ThreadTable::open()
{
   columns.emplace("thr_id", [&](Column& col) { col << counter->t_id.load(); });
   columns.emplace("tx_k", [&](Column& col) { col << loadResetValue(counter->tx) / KILO; });
   columns.emplace("tx_abort_k", [&](Column& col) { col << loadResetValue(counter->tx_abort) / KILO; });
   columns.emplace("exec_cycl_k", [&](Column& col) { col << loadResetValue(counter->exec_cycles) / KILO; });
   columns.emplace("exec_cycl_max_us", [&](Column& col) { col << loadResetValue(counter->exec_cycl_max_dur) / 2 / 1000; });
   columns.emplace("exec_cycl_max_poll_us", [&](Column& col) { col << loadResetValue(counter->exec_cycl_max_poll_us) / 2 / 1000; });
   columns.emplace("exec_cycl_max_subm_us", [&](Column& col) { col << loadResetValue(counter->exec_cycl_max_subm_us) / 2 / 1000; });
   columns.emplace("exec_cycl_max_task_us", [&](Column& col) { col << loadResetValue(counter->exec_cycl_max_task_us) / 2 / 1000; });
   columns.emplace("exec_tasks_run_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_run) / KILO; });
   columns.emplace("exec_tasks_run_no_k", [&](Column& col) { col << loadResetValue(counter->exec_no_tasks_run) / KILO; });
   columns.emplace("exec_tasks_st_done_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_st_comp) / KILO; });
   columns.emplace("exec_tasks_st_wait_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_st_wait) / KILO; });
   columns.emplace("exec_tasks_st_waitio_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_st_wait_io) / KILO; });
   columns.emplace("exec_tasks_st_ready_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_st_ready) / KILO; });
   columns.emplace("exec_tasks_st_readymem_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_st_ready_mem) / KILO; });
   columns.emplace("exec_tasks_st_readylck_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_st_ready_lck) / KILO; });
   columns.emplace("exec_tasks_st_readylckskip_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_st_ready_lckskip) / KILO; });
   columns.emplace("exec_tasks_st_readyjumplck_k", [&](Column& col) { col << loadResetValue(counter->exec_tasks_st_ready_jumplck) / KILO; });
   columns.emplace("pp_p1_picked_k", [&](Column& col) { col << loadResetValue(counter->pp_p1_picked) / KILO; });
   columns.emplace("pp_p23_evicted_k", [&](Column& col) { col << loadResetValue(counter->pp_p23_evicted) / KILO; });
   columns.emplace("pp_p2_iopushed_k", [&](Column& col) { col << loadResetValue(counter->pp_p2_iopushed) / KILO; });
}
// -------------------------------------------------------------------------------------
void ThreadTable::next()
{
   clear();
   for (auto& c: ThreadCounters::thread_counters) {
      counter = &c;
      for (auto& c : columns) {
         c.second.generator(c.second);
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
