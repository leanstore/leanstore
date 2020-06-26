#include "leanstore/threads/UT.hpp"

#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <emmintrin.h>
#include <errno.h>
#include <fcntl.h>
#include <libaio.h>
#include <linux/futex.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <ucontext.h>
#include <unistd.h>

#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace leanstore;
using namespace leanstore::threads;
using namespace leanstore::buffermanager;
using leanstore::buffermanager::PAGE_SIZE;
// -------------------------------------------------------------------------------------
struct SIO {
  int ssd_fd = -1;
  SIO()
  {
    int flags = O_RDWR;
    if (FLAGS_trunc) {
      flags |= O_TRUNC | O_CREAT;
    }
    ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
    posix_check(ssd_fd > -1);
    if (FLAGS_falloc > 0) {
      const u64 gib_size = 1024ull * 1024ull * 1024ull;
      auto dummy_data = (u8*)aligned_alloc(512, gib_size);
      for (u64 i = 0; i < FLAGS_falloc; i++) {
        const int ret = pwrite(ssd_fd, dummy_data, gib_size, gib_size * i);
        posix_check(ret == gib_size);
      }
      free(dummy_data);
      fsync(ssd_fd);
    }
    ensure(fcntl(ssd_fd, F_GETFL) != -1);
  }
  void read(u64 pid, u8* destination)
  {
    const int fd = ssd_fd;
    UserThreadManager::sleepThenCall([&, fd, pid, destination](std::function<void()> revive) {
      std::thread t([&, pid, destination, fd, revive]() {
        const int bytes_read = pread(fd, destination, PAGE_SIZE, pid * PAGE_SIZE);
        ensure(bytes_read == PAGE_SIZE);
        revive();
      });
      t.detach();
    });
  }
  void write(u64 pid, u8* src)
  {
    const int fd = ssd_fd;
    UserThreadManager::sleepThenCall([&, fd, pid, src](std::function<void()> revive) {
      std::thread t([&, pid, src, fd, revive]() {
        const int bytes_read = pwrite(fd, src, PAGE_SIZE, pid * PAGE_SIZE);
        ensure(bytes_read == PAGE_SIZE);
        revive();
      });
      t.detach();
    });
  }
};
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  UserThreadManager::init(FLAGS_worker_threads);
  if (0) {
    UserThreadManager::addThread([&]() {
      cout << "Hello from user thread " << endl;
      UserThreadManager::sleepThenCall([&](std::function<void()> revive) {
        std::thread t([&, revive]() {
          sleep(2);
          cout <<"time to wakeup" <<endl;
          revive();
        });
        t.detach();
      });
      cout << "Bye bye from user thread " << endl;
    });
  } else {
    for (u64 i = 0; i < FLAGS_y; i++)
      UserThreadManager::addThread([&, i]() { cout << "Hello from user thread " << i << endl; });

    SIO sio;
    UserThreadManager::addThread([&]() {
      cout << "writing thread" << endl;
      u8 page[PAGE_SIZE];
      page[0] = 'A';
      sio.write(0, page);
      cout << "done writing" << endl;
      UserThreadManager::addThread([&]() {
        u8 page[PAGE_SIZE];
        sio.read(0, page);
        cout << page[0] << endl;
        ensure(page[0] == 'A');
      });
    });
  }
  UserThreadManager::destroy();
  return 0;
}
