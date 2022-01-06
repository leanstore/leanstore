#pragma once
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>

using namespace leanstore;
template <class Record>
struct LeanStoreAdapter {
   storage::btree::BTreeInterface* btree;
   std::map<std::string, Record> map;
   string name;
   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name) : name(name)
   {
      if (FLAGS_recover) {
         btree = &db.retrieveBTreeLL(name);
      } else {
         btree = &db.registerBTreeLL(name);
      }
   }
   // -------------------------------------------------------------------------------------
   void printTreeHeight() { cout << name << " height = " << btree->getHeight() << endl; }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scanDesc(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      btree->scanDesc(
          folded_key, folded_key_len,
          [&](const u8* key, [[maybe_unused]] u16 key_length, const u8* payload, [[maybe_unused]] u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             typename Record::Key typed_key;
             Record::unfoldRecord(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return fn(typed_key, typed_payload);
          },
          undo);
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& rec_key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, rec_key);
      const auto res = btree->insert(folded_key, folded_key_len, (u8*)(&record), sizeof(Record));
      ensure(res == btree::OP_RESULT::OK || res == btree::OP_RESULT::ABORT_TX);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }

   template <class Fn>
   void lookup1(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = btree->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         assert(payload_length == sizeof(Record));
         fn(typed_payload);
      });
      ensure(res == btree::OP_RESULT::OK);
   }

   template <class Fn>
   void update1(const typename Record::Key& key, const Fn& fn, storage::btree::WALUpdateGenerator wal_update_generator)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = btree->updateSameSize(
          folded_key, folded_key_len,
          [&](u8* payload, u16 payload_length) {
             static_cast<void>(payload_length);
             assert(payload_length == sizeof(Record));
             Record& typed_payload = *reinterpret_cast<Record*>(payload);
             fn(typed_payload);
          },
          wal_update_generator);
      ensure(res != btree::OP_RESULT::NOT_FOUND);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }

   bool erase(const typename Record::Key& key)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const auto res = btree->remove(folded_key, folded_key_len);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
      return (res == btree::OP_RESULT::OK);
   }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scan(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      btree->scanAsc(
          folded_key, folded_key_len,
          [&](const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             static_cast<void>(payload_length);
             typename Record::Key typed_key;
             Record::unfoldRecord(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
             return fn(typed_key, typed_payload);
          },
          undo);
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   auto lookupField(const typename Record::Key& key, Field Record::*f)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      Field local_f;
      const auto res = btree->lookup(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         assert(payload_length == sizeof(Record));
         Record& typed_payload = *const_cast<Record*>(reinterpret_cast<const Record*>(payload));
         local_f = (typed_payload).*f;
      });
      ensure(res == btree::OP_RESULT::OK);
      return local_f;
   }

   uint64_t count() { return btree->countEntries(); }
};
