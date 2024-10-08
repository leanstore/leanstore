#include "benchmark/utils/scheduler.h"
#include "common/rand.h"

#include <algorithm>
#include <cassert>
#include <thread>

namespace benchmark {

thread_local uint64_t PoissonScheduler::prev_tsc = 0;
thread_local std::mt19937 PoissonScheduler::generator{std::hash<std::thread::id>{}(std::this_thread::get_id())};

/* Convert rate: txn per second -> txn per timestamp counter*/
PoissonScheduler::PoissonScheduler(double txn_per_sec)
    : rate_(txn_per_sec), dist_(txn_per_sec / (1000000000UL * tsctime::TSC_PER_NS)) {}

auto PoissonScheduler::IsEnable() -> bool { return rate_ != 0; }

/**
 * @brief Wait according to Poisson/exponential distribution to simulate real workloads,
 *  i.e. transactions not arrive immediately after the previous one completed.
 *
 * During the wait, the system (workers) become idle.
 * And during the idle period, the database system could trigger some maintainance/system tasks -- idle_fn
 *
 * @param idle_fn The maintainance task to be executed during idle
 * @return uint64_t The arrival time of the next transaction
 */
auto PoissonScheduler::Wait(const std::function<void()> &idle_fn) -> uint64_t {
  if (!IsEnable()) { return 0; }

  /* Initialize scheduling env */
  auto now_tsc       = tsctime::ReadTSC();
  auto expected_diff = static_cast<uint64_t>(dist_(generator));
  if (prev_tsc == 0) { prev_tsc = now_tsc - expected_diff; }

  /**
   * @brief Model the arrival time as below:
   *
   *    |----------- expected_diff -----------|
   *    |<- tx exec ->|<--    free_time    -->|
   * prev_tsc       now_tsc            next arrival time
   *
   * Therefore, if only we have free time between two txns: to_run_idle_fn == true
   *  we will run the idle_fn() ONCE, and mitigate the next arrival time accordingly
   */

  /* Environment for idle_fn() */
  auto idle_fn_exec_time = 0UL;

  /* Wait until the timestamp counter reaches its expected value: prev_tsc + expected_diff */
  while (now_tsc < prev_tsc + expected_diff) {
    ::_mm_pause();
    now_tsc             = tsctime::ReadTSC();
    auto before_idle_fn = now_tsc;
    idle_fn();
    now_tsc = tsctime::ReadTSC();
    idle_fn_exec_time += now_tsc - before_idle_fn;
  }

  // Long latency caused by the OS scheduler, so we reset start time to make it fairer
  if (tsctime::TscDifferenceS(prev_tsc, now_tsc) >= 1) {
    // if idle_fn affects arrival time negatively, we set the arrival time earlier a bit (fairer benchmark)
    prev_tsc = now_tsc - std::max(expected_diff, idle_fn_exec_time);
  }

  // advance the current timestamp counter
  prev_tsc += expected_diff;
  return prev_tsc;
}

}  // namespace benchmark