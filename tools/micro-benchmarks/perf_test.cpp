#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/futex.h>
#include <pthread.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  {
    int fd = open("./ssd", O_RDWR, 0666);
    const u64 size = 1024 * 16;
    auto buffer = std::make_unique<u8[]>(size);
    pwrite(fd, buffer.get(), size, 0);
    PerfEvent e;
    e.setParam("op", "pread");
    const u64 n = 1e6;
    atomic<u64> counter = 0;
    {
      PerfEventBlock b(e, n);
      for (u64 i = 0; i < n; i++) {
        posix_check(pread(fd, buffer.get(), size, 0) == size);
        for (u64 i = 0; i < 100; i++) {
          counter += buffer[i];
        }
      }
    }
    close(fd);
  }
  return 0;
}
