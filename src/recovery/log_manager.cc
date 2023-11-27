#include "recovery/log_manager.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"

namespace leanstore::recovery {

LogManager::LogManager(std::atomic<bool> &is_running) {
  logger_ = static_cast<LogWorker *>(aligned_alloc(BLK_BLOCK_SIZE, FLAGS_worker_count * sizeof(LogWorker)));
  for (size_t idx = 0; idx < FLAGS_worker_count; idx++) {
    logger_[idx].Init(is_running);
    Ensure((u64)(&logger_[idx]) % BLK_BLOCK_SIZE == 0);
  }
  assert(logger_ != nullptr);
}

LogManager::~LogManager() {
  for (size_t idx = 0; idx < FLAGS_worker_count; idx++) { logger_[idx].~LogWorker(); }
  free(logger_);
}

auto LogManager::LocalLogWorker() -> LogWorker & {
  assert(worker_thread_id < FLAGS_worker_count);
  return logger_[worker_thread_id];
}

}  // namespace leanstore::recovery