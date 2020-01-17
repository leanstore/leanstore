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
#include <set>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_double(target_gib, 1, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_uint32(ycsb_tx_rounds, 1, "");
DEFINE_uint32(ycsb_tx_count, 0, "default = tuples");
DEFINE_bool(fs, false, "");
DEFINE_bool(verify, false, "");
DEFINE_bool(ycsb_scan, false, "");
DEFINE_bool(ycsb_tx, true, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
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
  // -------------------------------------------------------------------------------------
  auto& table = *adapter;
  const u64 ycsb_tuple_count =
      (FLAGS_ycsb_tuple_count) ? FLAGS_ycsb_tuple_count : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
  // Insert values
  {
    const u64 n = ycsb_tuple_count;
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "Inserting values" << endl;
    begin = chrono::high_resolution_clock::now();
    {
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
        vector<u64> keys(range.size());
        std::iota(keys.begin(), keys.end(), range.begin());
        std::random_shuffle(keys.begin(), keys.end());
        for (const auto& key : keys) {
          YCSBPayload payload;
          utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
          table.insert(key, payload);
        }
      });
    }
    end = chrono::high_resolution_clock::now();
    cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
    cout << calculateMTPS(begin, end, n) << " M tps" << endl;
    // -------------------------------------------------------------------------------------
    const u64 written_pages = db.getBufferManager().consumedPages();
    const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
    cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  // -------------------------------------------------------------------------------------
  // Prepare lookup_keys in Zipf distribution
  const u64 tx_count = (FLAGS_ycsb_tx_count) ? FLAGS_ycsb_tx_count : ycsb_tuple_count;
  vector<YCSBKey> lookup_keys(tx_count);
  std::set<u64> lookup_unique_keys;
  // -------------------------------------------------------------------------------------
  cout << "-------------------------------------------------------------------------------------" << endl;
  cout << "Preparing Lookup Keys" << endl;
  {
    const string lookup_keys_file =
        FLAGS_zipf_path + "/ycsb_" + to_string(tx_count) + "_lookup_keys_" + to_string(sizeof(YCSBKey)) + "b_zipf_" + to_string(FLAGS_zipf_factor);
    if (utils::fileExists(lookup_keys_file)) {
      utils::fillVectorFromBinaryFile(lookup_keys_file.c_str(), lookup_keys);
      if (FLAGS_ycsb_count_unique_lookup_keys) {
        for (const auto& key : lookup_keys) {
          lookup_unique_keys.insert(key);
        }
        cout << "unique lookup keys = " << lookup_unique_keys.size() << endl;
      }
    } else {
      auto random = std::make_unique<utils::ZipfRandom>(ycsb_tuple_count, FLAGS_zipf_factor);
      std::generate(lookup_keys.begin(), lookup_keys.end(), [&]() {
        u64 key = random->rand() % (ycsb_tuple_count);
        lookup_unique_keys.insert(key);
        return key;
      });
      utils::writeBinary(lookup_keys_file.c_str(), lookup_keys);
      cout << "unique lookup keys = " << lookup_unique_keys.size() << endl;
    }
  }
  cout << setprecision(4);
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  // Scan
  if (FLAGS_ycsb_scan) {
    const u64 n = ycsb_tuple_count;
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "Scan" << endl;
    {
      begin = chrono::high_resolution_clock::now();
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
        for (u64 i = range.begin(); i < range.end(); i++) {
          YCSBPayload result;
          table.lookup(i, result);
        }
      });
      end = chrono::high_resolution_clock::now();
    }
    // -------------------------------------------------------------------------------------
    cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
    // -------------------------------------------------------------------------------------
    cout << calculateMTPS(begin, end, n) << " M tps" << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  // -------------------------------------------------------------------------------------
  const u64 n = lookup_keys.size();
  cout << "-------------------------------------------------------------------------------------" << endl;
  cout << "~Transactions" << endl;
  while (true) {
    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      for (u64 i = range.begin(); i < range.end(); i++) {
        YCSBKey key = lookup_keys[i];
        YCSBPayload result;
        if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
          table.lookup(key, result);
        } else {
          YCSBPayload payload;
          utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
          table.update(key, payload);
        }
        WorkerCounters::myCounters().tx++;
      }
    });
  }
  cout << "-------------------------------------------------------------------------------------" << endl;
  // -------------------------------------------------------------------------------------
  return 0;
}
