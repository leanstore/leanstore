#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_string(in, "", "");
DEFINE_string(out, "", "");
DEFINE_string(op, "convert", "");
DEFINE_string(generator, "scrambled", "");
DEFINE_uint64(count, 1000, "");
DEFINE_uint64(n, 1000, "");
DEFINE_uint64(repeat, 2, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = BytesPayload<128>;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("Leanstore Frontend");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  if (FLAGS_op == "convert") {
    vector<u64> keys;
    utils::fillVectorFromBinaryFile(FLAGS_in.c_str(), keys);
    std::ofstream csv;
    csv.open(FLAGS_out, ios::out | ios::trunc);
    csv << std::setprecision(2) << std::fixed << "i,k" << std::endl;
    for (u64 i = 0; i < keys.size(); i++) {
      csv << i << "," << keys[i] << std::endl;
    }
  } else if (FLAGS_op == "generate") {
    std::ofstream csv;
    csv.open(FLAGS_out, ios::out | ios::trunc);
    csv << std::setprecision(2) << std::fixed << "i,k" << std::endl;
    if (FLAGS_generator == "zipf") {
      auto random = std::make_unique<utils::ZipfGenerator>(FLAGS_n, FLAGS_zipf_factor);
      for (u64 i = 0; i < FLAGS_count * FLAGS_repeat; i++) {
        csv << i << "," << random->rand() << std::endl;
      }
    } else if (FLAGS_generator == "scrambled") {
      auto random = std::make_unique<utils::ScrambledZipfGenerator>(0, FLAGS_n, FLAGS_zipf_factor);
      for (u64 i = 0; i < FLAGS_count * FLAGS_repeat; i++) {
        csv << i << "," << random->rand() << std::endl;
      }
    } else {
      ensure(false);
    }
  } else if (FLAGS_op == "perf") {
    PerfEvent e;
    u64 sum = 0;
    if (FLAGS_generator == "zipf") {
      auto random = std::make_unique<utils::ZipfGenerator>(FLAGS_n, FLAGS_zipf_factor);
      PerfEventBlock b(e, FLAGS_count);
      for (u64 i = 0; i < FLAGS_count; i++) {
        sum += random->rand();
      }
    } else if (FLAGS_generator == "scrambled") {
      auto random = std::make_unique<utils::ScrambledZipfGenerator>(0, FLAGS_n, FLAGS_zipf_factor);
      PerfEventBlock b(e, FLAGS_count);
      for (u64 i = 0; i < FLAGS_count; i++) {
        sum += random->rand();
      }
    }
    cout << sum << endl;
  }
  // -------------------------------------------------------------------------------------
  return 0;
}
