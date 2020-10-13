#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
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
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace threads
{
struct UserThread {
   bool init = false;
   ucontext_t context;
   unique_ptr<u8[]> stack;
   std::function<void()> run;
   s64 worker_id = -1;
};
struct UserThreadManager {
   static atomic<bool> keep_running;
   static atomic<u64> running_threads;
   static std::mutex utm_mutex;
   static std::vector<UserThread> uts;
   static std::vector<u64> uts_ready, uts_blocked;
   static std::vector<std::thread> worker_threads;
   static void init(u64);
   static void destroy();
   static void addThread(std::function<void()> run);
   static void sleepThenCall(std::function<void(std::function<void()>)> work);
};
using UT = UserThread;
using UTM = UserThreadManager;
}  // namespace threads
}  // namespace leanstore
