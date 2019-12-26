#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfRandom.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>
#include "PerfEvent.hpp"
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
  PerfEvent e;
  e.setParam("threads", FLAGS_worker_threads);
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
  cout << "-------------------------------------------------------------------------------------" << endl;
  cout << "Preparing Workload" << endl;
  {
    const string lookup_keys_file =
        FLAGS_zipf_path + "ycsb_" + to_string(tx_count) + "_lookup_keys_" + to_string(sizeof(YCSBKey)) + "b_zipf_" + to_string(FLAGS_zipf_factor);
    if (utils::fileExists(lookup_keys_file)) {
      utils::fillVectorFromBinaryFile(lookup_keys_file.c_str(), lookup_keys);
    } else {
      auto random = std::make_unique<utils::ZipfRandom>(FLAGS_ycsb_tuple_count, FLAGS_zipf_factor);
      std::generate(lookup_keys.begin(), lookup_keys.end(), [&]() { return random->rand() % (FLAGS_ycsb_tuple_count); });
      utils::writeBinary(lookup_keys_file.c_str(), lookup_keys);
    }
  }
  // -------------------------------------------------------------------------------------
  // Prepare Payload
  {
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "Preparing Payload" << endl;
    const string payload_file = FLAGS_zipf_path + "ycsb_payload_" + to_string(FLAGS_ycsb_tuple_count) + "_" + to_string(sizeof(YCSBPayload)) + "b";
    if (utils::fileExists(payload_file)) {
      utils::fillVectorFromBinaryFile(payload_file.c_str(), payloads);
    } else {
      tbb::parallel_for(tbb::blocked_range<u64>(0, payloads.size()), [&](const tbb::blocked_range<u64>& range) {
        for (u64 i = range.begin(); i < range.end(); i++) {
          utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(payloads.data() + i), sizeof(YCSBPayload));
        }
      });
      utils::writeBinary(payload_file.c_str(), payloads);
    }
  }
  cout << "-------------------------------------------------------------------------------------" << endl;
  cout << setprecision(4);
  // -------------------------------------------------------------------------------------
  // Insert values
  {
    const u64 n = FLAGS_ycsb_tuple_count;
    e.setParam("op", "insert");
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "Inserting values" << endl;
    begin = chrono::high_resolution_clock::now();
    {
      PerfEventBlock b(e, n);
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
        for (u64 i = range.begin(); i < range.end(); i++) {
          YCSBPayload& payload = payloads[i];
          table.insert(i, payload);
        }
      });
    }
    end = chrono::high_resolution_clock::now();
    cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
    cout << calculateMTPS(begin, end, n) << " M tps" << endl;
    // -------------------------------------------------------------------------------------
    const u64 written_pages = db.getBufferManager().consumedPages();
    const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
    cout << "inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  // Scan
  if (FLAGS_ycsb_scan) {
    e.setParam("op", "scan");
    const u64 n = FLAGS_ycsb_tuple_count;
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "Scan" << endl;
    {
      begin = chrono::high_resolution_clock::now();
      PerfEventBlock b(e, n);
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
  if (FLAGS_verify) {
    const u64 n = FLAGS_ycsb_tuple_count;
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "Verification" << endl;
    begin = chrono::high_resolution_clock::now();
    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      for (u64 i = range.begin(); i < range.end(); i++) {
        YCSBPayload result;
        ensure(table.lookup(i, result) && result == payloads[i]);
      }
    });
    end = chrono::high_resolution_clock::now();
    cout << calculateMTPS(begin, end, n) << " M tps" << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  // -------------------------------------------------------------------------------------
  if (FLAGS_ycsb_tx) {
    const u64 n = lookup_keys.size();
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "~Transactions" << endl;
    e.setParam("op", "tx");
    PerfEventBlock b(e, lookup_keys.size() * (FLAGS_ycsb_warmup_rounds + FLAGS_ycsb_tx_rounds));
    for (u32 r_i = 0; r_i < (FLAGS_ycsb_warmup_rounds + FLAGS_ycsb_tx_rounds); r_i++) {
      begin = chrono::high_resolution_clock::now();
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
        for (u64 i = range.begin(); i < range.end(); i++) {
          YCSBKey key = lookup_keys[i];
          YCSBPayload result;
          if (utils::RandomGenerator::getRandU64(0, 100) <= FLAGS_ycsb_read_ratio) {
            table.lookup(key, result);
          } else {
            const u32 rand_payload = utils::RandomGenerator::getRand<u32>(0, FLAGS_ycsb_tuple_count);
            table.update(key, payloads[rand_payload]);
            if (FLAGS_verify) {
              ensure(table.lookup(key, result) && result == payloads[rand_payload]);
            }
          }
        }
      });
      end = chrono::high_resolution_clock::now();
      // -------------------------------------------------------------------------------------
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      // -------------------------------------------------------------------------------------
      if (r_i < FLAGS_ycsb_warmup_rounds) {
        cout << "Warmup: ";
      } else {
        cout << "Hot run: ";
      }
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
    }
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  // -------------------------------------------------------------------------------------
  return 0;
}
