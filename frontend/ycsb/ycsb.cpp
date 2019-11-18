#include "Units.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/ZipfRandom.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>
#include <gflags/gflags.h>
#include "PerfEvent.hpp"
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_threads, 20, "");
DEFINE_double(ycsb_zipf_factor, 1.0, "");
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 100000, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 1, "");
DEFINE_uint32(ycsb_tx_rounds, 1, "");
DEFINE_uint32(ycsb_tx_count, 0, "default = tuples");
DEFINE_bool(persist, true, ""); // TODO: still not ready
DEFINE_bool(verify, false, "");
DEFINE_bool(ycsb_scan, true, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
typedef struct YCSBPayload {
   u8 value[120];
   YCSBPayload() {}
   bool operator==(YCSBPayload &other)
   {
      return (std::memcmp(value, other.value, sizeof(value)) == 0);
   }
   bool operator!=(YCSBPayload &other)
   {
      return !(operator==(other));
   }
   YCSBPayload(const YCSBPayload &other)
   {
      std::memcpy(value, other.value, sizeof(value));
   }
};
int main(int argc, char **argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   tbb::task_scheduler_init taskScheduler(FLAGS_ycsb_threads);
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   LeanStore db;
   btree::BTree<YCSBKey, YCSBPayload> *btree_ptr;
   if ( FLAGS_persist ) {
      btree_ptr = &db.registerBTree<YCSBKey, YCSBPayload>("ycsb");
   } else {
      db.restore();
      btree_ptr = &db.locateBTree<YCSBKey, YCSBPayload>("ycsb");
   }
   auto &table = *btree_ptr;
   // -------------------------------------------------------------------------------------
   // Print fanout information
   table.printFanoutInformation();
   // -------------------------------------------------------------------------------------
   // Prepare lookup_keys in Zipf distribution
   const u32 tx_count = (FLAGS_ycsb_tx_count) ? FLAGS_ycsb_tx_count : FLAGS_ycsb_tuple_count;
   vector<YCSBKey> lookup_keys(tx_count);
   vector<YCSBPayload> payloads(FLAGS_ycsb_tuple_count);
   // -------------------------------------------------------------------------------------
   cout << "-------------------------------------------------------------------------------------" << endl;
   cout << "Preparing Workload" << endl;
   {
      const string lookup_keys_file = "ycsb_" + to_string(tx_count) + "_lookup_keys_" + to_string(sizeof(YCSBKey)) + "b_zipf_" + to_string(FLAGS_ycsb_zipf_factor);
      if ( utils::fileExists(lookup_keys_file)) {
         utils::fillVectorFromBinaryFile(lookup_keys_file.c_str(), lookup_keys);
      } else {
         auto random = std::make_unique<utils::ZipfRandom>(FLAGS_ycsb_tuple_count, FLAGS_ycsb_zipf_factor);
         std::generate(lookup_keys.begin(), lookup_keys.end(), [&]() { return random->rand() % (FLAGS_ycsb_tuple_count); });
         utils::writeBinary(lookup_keys_file.c_str(), lookup_keys);
      }
   }
   // -------------------------------------------------------------------------------------
   // Prepare Payload
   {
      const string payload_file = "ycsb_payload_" + to_string(FLAGS_ycsb_tuple_count) + "_" + to_string(sizeof(YCSBPayload)) + "b";
      if ( utils::fileExists(payload_file)) {
         utils::fillVectorFromBinaryFile(payload_file.c_str(), payloads);
      } else {
         for ( u64 t_i = 0; t_i < payloads.size(); t_i++ ) {
            utils::RandomGenerator::getRandString(reinterpret_cast<u8 *>(payloads.data() + t_i), sizeof(YCSBPayload));
         }
         utils::writeBinary(payload_file.c_str(), payloads);
      }
   }
   cout << "-------------------------------------------------------------------------------------" << endl;
   // -------------------------------------------------------------------------------------
   // Insert values
   if ( FLAGS_persist ) {
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Inserting values" << endl;
      auto begin = chrono::high_resolution_clock::now();
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, FLAGS_ycsb_tuple_count), [&](const tbb::blocked_range<uint32_t> &range) {
         for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
            YCSBPayload &payload = payloads[i];
            table.insert(i, payload);
         }
      });
      auto end = chrono::high_resolution_clock::now();
      u32 tps = (u32) ((lookup_keys.size() * 1.0 / chrono::duration_cast<chrono::microseconds>(end - begin).count()) * 1000 * 1000);
      cout << tps * 1.0 / 1e6 << " M tps" << endl;
      const u64 written_pages = db.getBufferManager().consumedPages();
      const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
      cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      cout << "needed/available = " << written_pages * 1.0 / (db.config.dram_pages_count) << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
      // -------------------------------------------------------------------------------------
      db.getBufferManager().flushDropAllPages();
   }
   // -------------------------------------------------------------------------------------
   // Scan
   if ( FLAGS_ycsb_scan ) {
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Scan" << endl;
      auto begin = chrono::high_resolution_clock::now();
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, FLAGS_ycsb_tuple_count), [&](const tbb::blocked_range<uint32_t> &range) {
         for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
            YCSBPayload result;
            ensure(table.lookup(i, result));
            if ( FLAGS_verify ) {
               YCSBPayload &should = payloads[i];
               explain(result == should);
            }
         }
      });
      auto end = chrono::high_resolution_clock::now();
      u32 tps = (u32) ((FLAGS_ycsb_tuple_count * 1.0 / chrono::duration_cast<chrono::microseconds>(end - begin).count()) * 1000 * 1000);
      cout << tps * 1.0 / 1e6 << " M tps" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   // -------------------------------------------------------------------------------------
   if ( !FLAGS_verify ) {
      PerfEvent e;
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "~Transactions" << endl;
      PerfEventBlock b(e, lookup_keys.size() * (FLAGS_ycsb_warmup_rounds + FLAGS_ycsb_tx_rounds));
      e.setParam("threads", FLAGS_ycsb_threads);
      for ( u32 r_i = 0; r_i < (FLAGS_ycsb_warmup_rounds + FLAGS_ycsb_tx_rounds); r_i++ ) {
         auto begin = chrono::high_resolution_clock::now();
         tbb::parallel_for(tbb::blocked_range<uint32_t>(0, lookup_keys.size()), [&](const tbb::blocked_range<uint32_t> &range) {
            for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
               YCSBKey key = lookup_keys[i];
               YCSBPayload result;
               if ( utils::RandomGenerator::getRand(0, 100) <= FLAGS_ycsb_read_ratio ) {
                  bool res = table.lookup(key, result);
               } else {
                  table.insert(key, payloads[key]);
               }
            }
         });
         auto end = chrono::high_resolution_clock::now();
         u32 tps = (u32) ((lookup_keys.size() * 1.0 / chrono::duration_cast<chrono::microseconds>(end - begin).count()) * 1000 * 1000);
         if ( r_i < FLAGS_ycsb_warmup_rounds ) {
            cout << "Warmup: ";
         } else {
            cout << "Hot run: ";
         }
         cout << tps * 1.0 / 1e6 << " M tps" << endl;
      }
      cout << "-------------------------------------------------------------------------------------" << endl;
   } else {
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Verification" << endl;
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, lookup_keys.size()), [&](const tbb::blocked_range<uint32_t> &range) {
         for ( uint32_t i = range.begin(); i < range.end(); i++ ) {
            YCSBKey key = lookup_keys[i];
            YCSBPayload result;
            if ( utils::RandomGenerator::getRand(0, 100) <= FLAGS_ycsb_read_ratio ) {
               bool res = table.lookup(key, result);
               if ( !res ) {
                  cerr << "not found !" << endl;
               }
               if ( result != payloads[key] ) {
                  cerr << " result != " << result.value[0] << endl;
               }
            } else {
               table.insert(key, payloads[key]);
            }
         }
      });
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   // -------------------------------------------------------------------------------------
   db.persist();
   return 0;
}