#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfRandom.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 100000, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 1, "");
DEFINE_uint32(ycsb_tx_rounds, 1, "");
DEFINE_uint32(ycsb_tx_count, 0, "default = tuples");
DEFINE_bool(fs, true, "");
DEFINE_bool(verify, false, "");
DEFINE_bool(ycsb_scan, false, "");
DEFINE_bool(ycsb_tx, true, "");
DEFINE_string(zipf_path, "/bulk/zipf", "");
DEFINE_double(zipf_factor, 1.0, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<120>;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
  double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
  return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("Leanstore Frontend");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  chrono::high_resolution_clock::time_point begin, end;
  // -------------------------------------------------------------------------------------
  // LeanStore DB
  LeanStore db;
  unique_ptr<BTreeInterface<YCSBKey, YCSBPayload>> adapter;
  if (FLAGS_fs) {
    auto& fs_btree = db.registerFSBTree<YCSBKey, YCSBPayload>("ycsb");
    adapter.reset(new BTreeFSAdapter(fs_btree));
  } else {
    auto& vs_btree = db.registerVSBTree("ycsb");
    adapter.reset(new BTreeVSAdapter<YCSBKey, YCSBPayload>(vs_btree));
  }
  auto& table = *adapter;
  // -------------------------------------------------------------------------------------
  // Prepare lookup_keys in Zipf distribution
  const u64 tx_count = (FLAGS_ycsb_tx_count) ? FLAGS_ycsb_tx_count : FLAGS_ycsb_tuple_count;
  vector<YCSBKey> lookup_keys(tx_count);
  vector<YCSBPayload> payloads(FLAGS_ycsb_tuple_count);
  // -------------------------------------------------------------------------------------
  {
      auto random = std::make_unique<utils::ZipfGenerator>(FLAGS_ycsb_tuple_count, FLAGS_zipf_factor);
      std::generate(lookup_keys.begin(), lookup_keys.end(), [&]() { return random->rand() % (FLAGS_ycsb_tuple_count); });
      for(auto &i : lookup_keys) {
        cout << i << endl;
      }
  }
  // -------------------------------------------------------------------------------------
  return 0;
}
