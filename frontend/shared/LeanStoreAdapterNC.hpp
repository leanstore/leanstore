#pragma once
#include "Adapter.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

using namespace leanstore;
using TID = u64;
std::atomic<TID> global_tid[1024] = {0};
// -------------------------------------------------------------------------------------
template <class Record>
struct LeanStoreAdapter : Adapter<Record> {
   leanstore::storage::btree::BTreeLL* key_tid;
   leanstore::storage::btree::BTreeLL* tid_value;
   string name;
   // -------------------------------------------------------------------------------------
   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name) : name(name)
   {
      key_tid = &db.registerBTreeLL(name + "_key_tid", false);
      tid_value = &db.registerBTreeLL(name + "_tid_value", false);
   }
   // -------------------------------------------------------------------------------------
   void printTreeHeight() {}
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      TID tid = global_tid[Record::id * 8].fetch_add(1);
      OP_RESULT res;
      res = key_tid->insert(folded_key, folded_key_len, (u8*)(&tid), sizeof(TID));
      ensure(res == leanstore::OP_RESULT::OK);
      res = tid_value->insert((u8*)&tid, sizeof(TID), (u8*)(&record), sizeof(Record));
      ensure(res == leanstore::OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   void moveIt(TID tid, u8* folded_key, u16 folded_key_len)
   {
      if (tid & (1ull << 63)) {
         return;
      }
      const bool move_it = FLAGS_nc_reallocation && leanstore::utils::RandomGenerator::getRandU64(0, FLAGS_tmp3) == 0;
      if (move_it) {
         UpdateSameSizeInPlaceDescriptor tmp;
         tmp.count = 0;
         OP_RESULT ret = key_tid->updateSameSizeInPlace(
             folded_key, folded_key_len,
             [&](u8* payload, u16 payload_length) {
                ensure(payload_length == sizeof(TID));
                TID old_tid = *reinterpret_cast<TID*>(payload);
                TID new_tid = global_tid[Record::id * 8].fetch_add(1) | (1ull << 63);
                // -------------------------------------------------------------------------------------
                u8 copy[16 * 1024];
                u64 copy_length;
                OP_RESULT ret2 = tid_value->lookup((u8*)&old_tid, sizeof(TID), [&](const u8* payload, u16 payload_length) {
                   ensure(payload_length == sizeof(Record));
                   copy_length = payload_length;
                   std::memcpy(copy, payload, copy_length);
                });
                if (ret2 != OP_RESULT::OK) {
                   return;
                }
                // -------------------------------------------------------------------------------------
                OP_RESULT ret3 = tid_value->insert((u8*)&new_tid, sizeof(TID), copy, copy_length);
                ensure(ret3 == OP_RESULT::OK);
                // -------------------------------------------------------------------------------------
                OP_RESULT ret4 = tid_value->remove((u8*)&old_tid, sizeof(TID));
                ensure(ret4 == OP_RESULT::OK);
                // -------------------------------------------------------------------------------------
                *reinterpret_cast<TID*>(payload) = new_tid;
             },
             tmp);
         ensure(ret == OP_RESULT::OK);  // As long as no one deletes the key
         if (FLAGS_tmp5) {
            cout << "moved " << tid << endl;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& cb) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      OP_RESULT ret;
      TID tid;
      ret = key_tid->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         ensure(payload_length == sizeof(TID));
         tid = *reinterpret_cast<const TID*>(payload);
         // -------------------------------------------------------------------------------------
         tid_value->lookup((u8*)&tid, sizeof(TID), [&](const u8* payload, u16 payload_length) {
            ensure(payload_length == sizeof(Record));
            const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
            cb(typed_payload);
         });
      });
      ensure(ret == OP_RESULT::OK);
      // -------------------------------------------------------------------------------------
      moveIt(tid, folded_key, folded_key_len);
   }
   // -------------------------------------------------------------------------------------
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& cb, UpdateSameSizeInPlaceDescriptor& update_descriptor) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      UpdateSameSizeInPlaceDescriptor tmp;
      tmp.count = 0;
      OP_RESULT ret;
      TID tid;
      ret = key_tid->updateSameSizeInPlace(
          folded_key, folded_key_len,
          [&](u8* tid_payload, u16 tid_payload_length) {
             ensure(tid_payload_length == sizeof(TID));
             tid = *reinterpret_cast<const TID*>(tid_payload);
             // -------------------------------------------------------------------------------------
             OP_RESULT ret2 = tid_value->updateSameSizeInPlace((u8*)&tid, sizeof(TID),
                                                               [&](u8* payload, u16 payload_length) {
                                                                  static_cast<void>(payload_length);
                                                                  assert(payload_length == sizeof(Record));
                                                                  Record& typed_payload = *reinterpret_cast<Record*>(payload);
                                                                  cb(typed_payload);
                                                               },
                                                               update_descriptor);
             ensure(ret2 == OP_RESULT::OK);
          },
          tmp);
      ensure(ret == OP_RESULT::OK);
      moveIt(tid, folded_key, folded_key_len);
   }
   // -------------------------------------------------------------------------------------
   bool erase(const typename Record::Key& key) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      OP_RESULT ret;
      TID tid;
      ret = key_tid->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         ensure(payload_length == sizeof(TID));
         tid = *reinterpret_cast<const TID*>(payload);
      });
      if (ret != OP_RESULT::OK) {
         return false;
      }
      // -------------------------------------------------------------------------------------
      ret = tid_value->remove((u8*)&tid, sizeof(TID));
      if (ret != OP_RESULT::OK) {
         return false;
      }
      ret = key_tid->remove(folded_key, folded_key_len);
      if (ret != OP_RESULT::OK) {
         return false;
      }
      return true;
   }
   // -------------------------------------------------------------------------------------
   void scan(const typename Record::Key& key,
             const std::function<bool(const typename Record::Key&, const Record&)>& cb,
             std::function<void()> undo) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      OP_RESULT ret;
      ret = key_tid->scanAsc(
          folded_key, folded_key_len,
          [&](const u8* key, [[maybe_unused]] u16 key_length, const u8* tid_ptr, [[maybe_unused]] u16 tid_length) {
             TID tid = *reinterpret_cast<const TID*>(tid_ptr);
             ensure(tid_length == sizeof(TID));
             // -------------------------------------------------------------------------------------
             bool should_continue;
             OP_RESULT res2 = tid_value->lookup((u8*)&tid, sizeof(TID), [&](const u8* value_ptr, u16 value_length) {
                ensure(value_length == sizeof(Record));
                typename Record::Key typed_key;
                Record::unfoldKey(key, typed_key);
                const Record& typed_payload = *reinterpret_cast<const Record*>(value_ptr);
                should_continue = cb(typed_key, typed_payload);
             });
             if (res2 == OP_RESULT::OK) {
                return should_continue;
             } else {
                return true;
             }
          },
          undo);
      ensure(ret == OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& cb,
                 std::function<void()> undo) final
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      OP_RESULT ret;
      ret = key_tid->scanDesc(
          folded_key, folded_key_len,
          [&](const u8* key, [[maybe_unused]] u16 key_length, const u8* tid_ptr, [[maybe_unused]] u16 tid_length) {
             const TID tid = *reinterpret_cast<const TID*>(tid_ptr);
             ensure(tid_length == sizeof(TID));
             // -------------------------------------------------------------------------------------
             bool should_continue;
             OP_RESULT res2 = tid_value->lookup((u8*)&tid, sizeof(TID), [&](const u8* value_ptr, u16 value_length) {
                ensure(value_length == sizeof(Record));
                typename Record::Key typed_key;
                Record::unfoldKey(key, typed_key);
                const Record& typed_payload = *reinterpret_cast<const Record*>(value_ptr);
                should_continue = cb(typed_key, typed_payload);
             });
             if (res2 == OP_RESULT::OK) {
                return should_continue;
             } else {
                return true;
             }
          },
          undo);
      ensure(ret == OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      // -------------------------------------------------------------------------------------
      OP_RESULT ret;
      TID tid;
      ret = key_tid->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         ensure(payload_length == sizeof(TID));
         tid = *reinterpret_cast<const TID*>(payload);
      });
      ensure(ret == OP_RESULT::OK);
      // -------------------------------------------------------------------------------------
      Field local_f;
      ret = tid_value->lookup((u8*)&tid, sizeof(TID), [&](const u8* payload, u16 payload_length) {
         ensure(payload_length == sizeof(Record));
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         local_f = (typed_payload).*f;
      });
      ensure(ret == OP_RESULT::OK);
      moveIt(tid, folded_key, folded_key_len);
      return local_f;
   }
   // -------------------------------------------------------------------------------------
   u64 count() { return 0; }
};
