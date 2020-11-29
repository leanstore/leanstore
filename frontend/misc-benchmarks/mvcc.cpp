#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/WALMacros.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using Payload = u8[128];
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   LeanStore db;
   db.startProfilingThread();
   auto& crm = db.getCRManager();
   storage::btree::BTree* btree;
   crm.scheduleJobSync(0, [&]() { btree = &db.registerBTree("mvcc"); });
   // -------------------------------------------------------------------------------------
   union {
      u64 x;
      u8 b[8];
   } k;
   k.x = 10;  // TODO: fold
   Payload p;
   struct value_t {
      u64 v1;
   };
   utils::RandomGenerator::getRandString(p, sizeof(Payload));
   // -------------------------------------------------------------------------------------
   using OP_RESULT = leanstore::storage::btree::OP_RESULT;
   // -------------------------------------------------------------------------------------
   auto insert = [&](u8* k, u16 kl, u16 vl, u8* v) { return (FLAGS_vi) ? btree->insertVI(k, kl, vl, v) : btree->insertVW(k, kl, vl, v); };
   auto lookup = [&](u8* k, u16 kl, function<void(const u8*, u16)> pc) {
      return (FLAGS_vi) ? btree->lookupVI(k, kl, pc) : btree->lookupVW(k, kl, pc);
   };

   auto update = [&](u8* k, u16 kl, function<void(u8*, u16)> cb, storage::btree::WALUpdateGenerator g) {
      return (FLAGS_vi) ? btree->updateVI(k, kl, cb, g) : btree->updateVW(k, kl, cb, g);
   };

   auto scanAsc = [&](u8* start_key, u16 key_length, function<bool(u8 * key, u16 key_length, u8 * value, u16 value_length)> callback,
                      function<void()> undo) {
      if (FLAGS_vi) {
         ensure(false);
      } else {
         btree->scanAscVW(start_key, key_length, callback, undo);
      }
   };
   auto scanDesc = [&](u8* start_key, u16 key_length, function<bool(u8 * key, u16 key_length, u8 * value, u16 value_length)> callback,
                       function<void()> undo) {
      if (FLAGS_vi) {
         ensure(false);
      } else {
         btree->scanDescVW(start_key, key_length, callback, undo);
      }
   };
   auto remove = [&](u8* k, u16 kl) { return (FLAGS_vi) ? btree->removeVI(k, kl) : btree->removeVW(k, kl); };
   // -------------------------------------------------------------------------------------
   if (FLAGS_tmp == 500) {
      constexpr u64 COUNT = 10;
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         for (u64 t_i = 0; t_i < COUNT; t_i++) {
            k.x = t_i;
            *reinterpret_cast<u64*>(p) = t_i * 2;
            const auto ret = insert(k.b, sizeof(k.x), sizeof(p), p);
            ensure(ret == OP_RESULT::OK);
         }
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         union {
            u64 x;
            u8 b[8];
         } k;
         for (u64 t_i = 0; t_i < COUNT; t_i++) {
            k.x = t_i;
            const auto ret = update(
                k.b, sizeof(k.b), [&](u8* payload, u16 payload_length) { *reinterpret_cast<u64*>(payload) = t_i * 4; }, WALUpdate1(value_t, v1));
            ensure(ret == OP_RESULT::OK);
         }
         sleep(3);
         cr::Worker::my().commitTX();
      });
      sleep(1);
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         k.x = 0;
         scanAsc(
             k.b, sizeof(k.x),
             [&](u8* key, u16 key_length, u8* value, u16 value_length) {
                cout << *reinterpret_cast<u64*>(key) << "=" << *reinterpret_cast<u64*>(value) << endl;
                return true;
             },
             [&]() {});
         cr::Worker::my().commitTX();
         sleep(5);
         cr::Worker::my().startTX();
         k.x = 0;
         scanAsc(
             k.b, sizeof(k.x),
             [&](u8* key, u16 key_length, u8* value, u16 value_length) {
                cout << *reinterpret_cast<u64*>(key) << "=" << *reinterpret_cast<u64*>(value) << endl;
                return true;
             },
             [&]() {});
         k.x = COUNT;
         scanDesc(
             k.b, sizeof(k.x),
             [&](u8* key, u16 key_length, u8* value, u16 value_length) {
                cout << *reinterpret_cast<u64*>(key) << "=" << *reinterpret_cast<u64*>(value) << endl;
                return true;
             },
             [&]() {});
         cr::Worker::my().commitTX();
      });

   } else if (FLAGS_tmp == 50) {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = insert(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = lookup(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
   } else if (FLAGS_tmp == 0) {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = insert(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = remove(k.b, sizeof(k.x));
         ensure(ret == OP_RESULT::OK);
         sleep(4);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = lookup(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      sleep(10);
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = lookup(k.b, sizeof(k.x), [&](const u8*, u16) {});
         ensure(ret != OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
   } else if (FLAGS_tmp == 10) {
      // transaction abort
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         *reinterpret_cast<u64*>(p) = 200;
         const auto ret = insert(k.b, sizeof(k.x), sizeof(p), p);
         sleep(3);
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      sleep(1);
      crm.scheduleJobAsync(1, [&]() {
         {
            cr::Worker::my().startTX();
            *reinterpret_cast<u64*>(p) = 200;
            {
               union {
                  u64 x;
                  u8 b[8];
               } k2;
               k2.x = 99;
               const auto ret = insert(k2.b, sizeof(k2.x), sizeof(p), p);
               ensure(ret == OP_RESULT::OK);
            }
            const auto ret = insert(k.b, sizeof(k.x), sizeof(p), p);
            ensure(ret == OP_RESULT::ABORT_TX);
            if (ret == OP_RESULT::ABORT_TX) {
               cr::Worker::my().abortTX();
            } else {
               cr::Worker::my().commitTX();
            }
         }
         sleep(5);
         {
            cr::Worker::my().startTX();
            *reinterpret_cast<u64*>(p) = 200;
            const auto ret = insert(k.b, sizeof(k.x), sizeof(p), p);
            cout << (int)ret << endl;
            ensure(ret == OP_RESULT::DUPLICATE);
            cr::Worker::my().commitTX();
         }
      });
      // -------------------------------------------------------------------------------------
      crm.joinAll();
   } else if (FLAGS_tmp == 1) {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         *reinterpret_cast<u64*>(p) = 200;
         const auto ret = insert(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = update(
             k.b, sizeof(k.b), [&](u8* payload, u16 payload_length) { *reinterpret_cast<u64*>(payload) = 100; }, WALUpdate1(value_t, v1));
         ensure(ret == OP_RESULT::OK);
         sleep(3);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = lookup(k.b, sizeof(k.x), [&](const u8* payload, u16) { cout << *reinterpret_cast<const u64*>(payload) << endl; });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         sleep(6);
         cr::Worker::my().startTX();
         const auto ret = lookup(k.b, sizeof(k.x), [&](const u8* payload, u16) { cout << *reinterpret_cast<const u64*>(payload) << endl; });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      crm.joinAll();
   } else {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         auto ret = insert(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == OP_RESULT::OK);
         ret = lookup(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(ret == OP_RESULT::OK);
         sleep(4);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = lookup(k.b, sizeof(k.x), [&](const u8*, u16) {});
         ensure(ret == OP_RESULT::NOT_FOUND);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      sleep(6);
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = remove(k.b, sizeof(k.x));
         ensure(ret == OP_RESULT::OK);
         sleep(3);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = lookup(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
   }
   crm.joinAll();
   // -------------------------------------------------------------------------------------
   return 0;
}
