#include "Exceptions.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   const u64 pages = FLAGS_target_gib * 1024 * 1024 * 1024 / PAGE_SIZE;
   int flags = O_RDWR | O_DIRECT;
   if (FLAGS_trunc) {
      flags |= O_TRUNC | O_CREAT;
   }
   int ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   posix_check(ssd_fd);
   vector<thread> threads;
   // -------------------------------------------------------------------------------------
   for (unsigned i = 0; i < FLAGS_worker_threads; i++) {
      threads.emplace_back([&]() {
         leanstore::buffermanager::BufferFrame::Page local_page;
         while (true) {
            const u64 pid = leanstore::utils::RandomGenerator::getRandU64(0, pages);
            const int bytes_read = pread(ssd_fd, local_page, PAGE_SIZE, pid * PAGE_SIZE);
            posix_check(bytes_read > 0);
            if (bytes_read != PAGE_SIZE)
               throw;
         }
      });
   }
   for (auto& thread : threads) {
      thread.join();
   }
   return 0;
}
