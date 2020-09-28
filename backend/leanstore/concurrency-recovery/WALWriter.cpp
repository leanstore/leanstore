#include "WALWriter.hpp"

#include "TXMG.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
void WALWriter::writeAndFlush() {
}
// -------------------------------------------------------------------------------------
WALWriter(s32 ssd_fd) : ssd_fd(ssd_fd)
{
  ioctl(ssd_fd, BLKGETSIZE64, &ssd_end_offset);
  // -------------------------------------------------------------------------------------
  assert(FLAGS_wal_writer_threads == 1);
  thread ww_thread([&]() {
    while (ww_threads_keep_running) {
      writeAndFlush();
    }
    ww_threads_counter--;
  });
  ww_threads_counter++;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(FLAGS_pp_threads + 0, &cpuset);
  posix_check(pthread_setaffinity_np(ww_thread.native_handle(), sizeof(cpu_set_t), &cpuset) == 0);
  ww_thread.detach();
}
// -------------------------------------------------------------------------------------
~WALWriter()
{
  ww_threads_keep_running = false;
  while (ww_threads_counter) {
  }
}
}  // namespace cr
}  // namespace leanstore
