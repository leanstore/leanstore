#include "../shared/LeanStoreAdapter.hpp"
#include "Schema.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/parallel_for.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <set>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_uint32(ycsb_insert_threads, 0, "");
DEFINE_uint32(ycsb_threads, 0, "");
DEFINE_bool(ycsb_deterministic, false, "");
DEFINE_uint32(ycsb_ops_per_tx, 1, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<8>;
using KVTable = Relation<YCSBKey, YCSBPayload>;
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
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   // Always init with the maximum number of threads (FLAGS_worker_threads)
   LeanStore db;
   auto& crm = db.getCRManager();
   LeanStoreAdapter<KVTable> table;
   crm.scheduleJobSync(0, [&]() { table = LeanStoreAdapter<KVTable>(db, "YCSB"); });
   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   db.registerConfigEntry("ycsb_threads", FLAGS_ycsb_threads);
   db.registerConfigEntry("ycsb_deterministic", FLAGS_ycsb_deterministic);
   db.registerConfigEntry("ycsb_ops_per_tx", FLAGS_ycsb_ops_per_tx);
   // -------------------------------------------------------------------------------------
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   const TX_MODE tx_type = TX_MODE::OLTP;
   // -------------------------------------------------------------------------------------
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   // Insert values
   const u64 n = ycsb_tuple_count;
   // -------------------------------------------------------------------------------------
   if (FLAGS_tmp4) {
      // -------------------------------------------------------------------------------------
      std::ofstream csv;
      csv.open("zipf.csv", ios::trunc);
      csv.seekp(0, ios::end);
      csv << std::setprecision(2) << std::fixed;
      std::unordered_map<u64, u64> ht;
      auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, ycsb_tuple_count, FLAGS_zipf_factor);
      for (u64 t_i = 0; t_i < (FLAGS_tmp4 ? FLAGS_tmp4 : 1e6); t_i++) {
         u64 key = zipf_random->rand();
         if (ht.find(key) == ht.end()) {
            ht[key] = 0;
         } else {
            ht[key]++;
         }
      }
      csv << "key,count" << endl;
      for (auto& [key, value] : ht) {
         csv << key << "," << value << endl;
      }
      cout << ht.size() << endl;
      return 0;
   }
   // -------------------------------------------------------------------------------------
   if (!FLAGS_recover) {
      cout << "Inserting " << ycsb_tuple_count << " values" << endl;
      begin = chrono::high_resolution_clock::now();
      utils::Parallelize::range(FLAGS_ycsb_insert_threads ? FLAGS_ycsb_insert_threads : FLAGS_worker_threads, n, [&](u64 t_i, u64 begin, u64 end) {
         crm.scheduleJobAsync(t_i, [&, begin, end]() {
            for (u64 i = begin; i < end; i++) {
               YCSBPayload payload;
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               YCSBKey key = i;
               cr::Worker::my().startTX(tx_type, isolation_level);
               table.insert({key}, {payload});
               cr::Worker::my().commitTX();
            }
         });
      });
      crm.joinAll();
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
   const u64 DISTANCE = 8 * (PAGE_SIZE / (sizeof(YCSBKey) + sizeof(YCSBPayload)));
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   cout << "~Transactions" << endl;
   db.startProfilingThread();
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   const u32 exec_threads = FLAGS_ycsb_threads ? FLAGS_ycsb_threads : FLAGS_worker_threads;
   UpdateDescriptorGenerator1(tabular_update_descriptor, KVTable, my_payload);
   auto btree_vi = reinterpret_cast<leanstore::storage::btree::BTreeVI*>(table.btree);
   for (u64 t_i = 0; t_i < exec_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&]() {
         jumpmuTry()
         {
            running_threads_counter++;
            YCSBPayload result;
            std::vector<std::unique_ptr<leanstore::storage::btree::BTreeExclusiveIterator>> d_iterators;
            for (u64 op_i = 0; op_i < FLAGS_ycsb_ops_per_tx; op_i++) {
               d_iterators.emplace_back(
                   new leanstore::storage::btree::BTreeExclusiveIterator(*static_cast<leanstore::storage::btree::BTreeGeneric*>(btree_vi)));
            }
            std::vector<YCSBKey> keys(FLAGS_ycsb_ops_per_tx, 0);
            for (u64 op_i = 0; op_i < FLAGS_ycsb_ops_per_tx; op_i++) {
              keys[op_i] = op_i * DISTANCE;  // zipf->random()
            }
            while (keep_running) {
               jumpmuTry()
               {
                  if (FLAGS_ycsb_deterministic) {
                     for (u64 op_i = 0; op_i < FLAGS_ycsb_ops_per_tx; op_i++) {
                        u8 folded_key[sizeof(YCSBKey)];
                        u16 folded_key_len = fold(folded_key, keys[op_i]);
                        btree_vi->prepareDeterministicUpdate(folded_key, folded_key_len, *d_iterators[op_i]);
                        ensure(d_iterators[op_i]->leaf.guard.state == leanstore::storage::GUARD_STATE::EXCLUSIVE);
                     }
                     // -------------------------------------------------------------------------------------
                     cr::Worker::my().startTX(tx_type, isolation_level);
                     cr::Worker::my().commitTX();
                     // -------------------------------------------------------------------------------------
                     for (u64 op_i = 0; op_i < FLAGS_ycsb_ops_per_tx; op_i++) {
                        u8 folded_key[sizeof(YCSBKey)];
                        u16 folded_key_len = fold(folded_key, keys[op_i]);
                        btree_vi->executeDeterministricUpdate(
                            folded_key, folded_key_len, *d_iterators[op_i],
                            [&](u8* payload, u16 payload_length) {
                               ensure(payload_length == sizeof(YCSBPayload));
                               *reinterpret_cast<YCSBPayload*>(payload) = result;
                            },
                            tabular_update_descriptor);
                        d_iterators[op_i]->reset();
                     }
                     WorkerCounters::myCounters().tx++;
                  } else {
                    std::random_shuffle(keys.begin(), keys.end());
                     cr::Worker::my().startTX(tx_type, isolation_level);
                     for (u64 op_i = 0; op_i < FLAGS_ycsb_ops_per_tx; op_i++) {
                        utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&result), sizeof(YCSBPayload));
                        // -------------------------------------------------------------------------------------
                        table.update1(
                            {keys[op_i]}, [&](KVTable& rec) { rec.my_payload = result; }, tabular_update_descriptor);
                     }
                     cr::Worker::my().commitTX();
                     WorkerCounters::myCounters().tx++;
                  }
               }
               jumpmuCatch()
               {
                  ensure(!FLAGS_ycsb_deterministic);
                  WorkerCounters::myCounters().tx_abort++;
               }
            }
            running_threads_counter--;
         }
         jumpmuCatch() { ensure(false); }
      });
   }
   // -------------------------------------------------------------------------------------
   {
      // Shutdown threads
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
      }
      crm.joinAll();
   }
   cout << "-------------------------------------------------------------------------------------" << endl;
   return 0;
}
