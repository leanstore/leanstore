#include "leanstore/threads/UT.hpp"

#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
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
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  UserThreadManager::init(FLAGS_worker_threads);
  for (u64 i = 0; i < FLAGS_y; i++)
    UserThreadManager::addThread([&, i]() { cout << "Hello from user thread " << i << endl; });

  std::atomic<bool> aio_ready = false;
  std::function<void()> to_revive;
  UserThreadManager::addThread([&]() {
    cout << "aio thread " << endl;
    UserThreadManager::asyncCall([&](std::function<void()> revive) {
      cout << "doing aio work" << endl;
      to_revive = revive;
      aio_ready = true;
    });
    cout << "last step" << endl;
  });

  while (!aio_ready) {
  }
  to_revive();
  cout << "destroy" << endl;
  UserThreadManager::destroy();
  return 0;
}
