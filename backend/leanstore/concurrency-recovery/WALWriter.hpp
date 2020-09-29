#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
class WALWriter
{
 private:
  const s32 ssd_fd;
  u64 ssd_end_offset;
  atomic<u64> ww_threads_counter = 0;
  atomic<bool> ww_threads_keep_running = true;
  // -------------------------------------------------------------------------------------
  void writeAndFlush();
 public:
  WALWriter(s32 ssd_fd);
  ~WALWriter();
};
}  // namespace cr
}  // namespace leanstore
