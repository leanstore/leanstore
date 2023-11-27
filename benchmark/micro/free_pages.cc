#include "prototype/free_page/blob_alloc.h"

#include "gflags/gflags.h"

#include <atomic>
#include <memory>
#include <random>
#include <thread>

DEFINE_uint32(free_page_variant, 1,
              "Select the Free page manager variant. 0: First-fit, 1: Bitmap, 2: Tree, 3: Tiering");
DEFINE_uint32(number_of_pages, 131072, "Number of pages, should be a power of 2");
DEFINE_uint64(exec_seconds_per_phase, 7, "Execution time");
DEFINE_uint64(sleep_per_op, 0, "Sleep time per operation in milisecond");

struct WorkloadConf {
  uint8_t max_tier;
  uint8_t alloc_ratio;
  uint8_t append_ratio;
  uint8_t padding;

  // delete ratio = 10 - alloc_ratio - append_ratio
  WorkloadConf(uint8_t max_tier, uint8_t alloc, uint8_t append)
      : max_tier(max_tier), alloc_ratio(alloc), append_ratio(append) {}
};

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Free Page manager");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<FreePageManager> man;
  switch (FLAGS_free_page_variant) {
    case 0: man = std::make_unique<FirstFitAllocator>(FLAGS_number_of_pages); break;
    case 1: man = std::make_unique<BitmapFreePages>(FLAGS_number_of_pages); break;
    case 2: man = std::make_unique<TreeFreePages>(FLAGS_number_of_pages); break;
    case 3: man = std::make_unique<TieringFreePages<BlobRep::MAX_TIER>>(FLAGS_number_of_pages); break;
    default: return 1;
  }

  srand(time(NULL));
  auto buf                                  = BufferManager(man.get());
  std::atomic<bool> is_running              = true;
  std::atomic<uint64_t> processed           = 0;
  std::atomic<uint64_t> failure             = 0;
  std::atomic<WorkloadConf> workload_config = WorkloadConf(3, 7, 1);

  /* Workload thread */
  std::thread workload([&]() {
    while (is_running) {
      int choice = rand() % 10;
      auto conf  = workload_config.load();

      if (choice < conf.alloc_ratio) {
        /* Alloc */
        auto rand_sz = (1 << (rand() % conf.max_tier)) - 1;
        auto success = buf.AllocBlob(rand_sz);
        if (success) {
          processed += 1;
        } else {
          failure += 1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_sleep_per_op));
      } else if ((conf.alloc_ratio <= choice) && (choice < conf.alloc_ratio + conf.append_ratio)) {
        /* Append */
        if (buf.blob_list.size() > 0) {
          auto rand_idx = rand() % buf.blob_list.size();
          auto success  = buf.AppendBlob(rand_idx, (1 << (rand() % (conf.max_tier / 3))));
          if (success) {
            processed += 1;
          } else {
            failure += 1;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_sleep_per_op));
        }
      } else {
        /* Delete */
        if (buf.blob_list.size() > 0) {
          processed += 1;
          auto rand_idx = rand() % buf.blob_list.size();
          buf.RemoveBlob(rand_idx);
          std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_sleep_per_op));
        }
      }
    }
    std::printf("Halt workload thread\n");
  });

  /* Statistic thread */
  std::thread stat_collector([&]() {
    int cnt = 0;
    std::printf("system,throughput,failure,alloc_cnt,phys_cnt\n");
    while (is_running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      // Progress stats
      auto progress = processed.exchange(0);
      auto fail     = failure.exchange(0);
      // Buffer stats
      auto alloc = man->alloc_cnt.load();
      auto phys  = man->physical_cnt.load();
      // Output
      std::printf("%s,%d,%lu,%lu,%lu,%lu\n", man->SystemName(), cnt++, progress, fail, alloc, phys);
    }
    std::printf("Halt Profiling thread\n");
  });

  std::printf("1st workload\n");
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_exec_seconds_per_phase));
  std::printf("2nd workload\n");
  workload_config.store(WorkloadConf(15, 4, 2));
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_exec_seconds_per_phase));

  is_running = false;
  workload.join();
  stat_collector.join();
}