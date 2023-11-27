#pragma once

#include "common/constants.h"
#include "common/typedefs.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <thread>
#include <vector>

namespace leanstore {

class WorkerPool {
 public:
  struct WorkerThreadEnv {
    std::mutex mutex;
    std::condition_variable cv;
    std::function<void()> job;
    bool wt_ready = true;   // Idle
    bool job_set  = false;  // Has job
    bool job_done = false;  // Job done
  };

  explicit WorkerPool(std::atomic<bool> &keep_running);
  ~WorkerPool();
  void Stop();

  // Schedule APIs
  void ScheduleAsyncJob(wid_t wid, const std::function<void()> &job);
  void ScheduleSyncJob(wid_t wid, const std::function<void()> &job);

  // Misc APIs
  auto WorkersCount() -> u32;
  void JoinAll();

 private:
  void SetJob(wid_t wid, const std::function<void()> &job);
  void JoinWorker(wid_t wid, std::function<bool(WorkerThreadEnv &)> condition);

  std::atomic<bool> *is_running_;
  std::atomic<u32> threads_counter_{0};
  std::vector<std::thread> workers_;
  WorkerThreadEnv worker_env_[MAX_NUMBER_OF_WORKER];
};

}  // namespace leanstore