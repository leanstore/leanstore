#pragma once
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
#include "leanstore/storage/btree/WALMacros.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>

using namespace leanstore;
template <class Record>
struct LeanStoreAdapter {
   storage::btree::BTree* btree;
   std::map<std::string, Record> map;
   string name;
   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name) : btree(&db.registerBTree(name)), name(name) {}
   // -------------------------------------------------------------------------------------
   void printTreeHeight() { cout << name << " height = " << btree->height << endl; }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scanDesc(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      btree->scanDescLL(
          folded_key, folded_key_len,
          [&](u8* key, [[maybe_unused]] u16 key_length, u8* payload, [[maybe_unused]] u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             assert(payload_length == sizeof(Record));
             typename Record::Key typed_key;
             Record::unfoldRecord(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<Record*>(payload);
             return fn(typed_key, typed_payload);
          },
          undo);
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& rec_key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, rec_key);
      btree->insertLL(folded_key, folded_key_len, sizeof(Record), (u8*)(&record));
   }

   template <class Fn>
   void lookup1(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      const bool found = btree->lookupOneLL(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         assert(payload_length == sizeof(Record));
         fn(typed_payload);
      });
      if (!found) {
         ensure(false);
      }
   }

   template <class Fn>
   void update1(const typename Record::Key& key, const Fn& fn, storage::btree::BTree::WALUpdateGenerator wal_update_generator)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      btree->updateSameSizeLL(
          folded_key, folded_key_len,
          [&](u8* payload, u16 payload_length) {
             static_cast<void>(payload_length);
             assert(payload_length == sizeof(Record));
             Record& typed_payload = *reinterpret_cast<Record*>(payload);
             fn(typed_payload);
          },
          wal_update_generator);
   }

   bool erase(const typename Record::Key& key)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      if (btree->lookupOneLL(folded_key, folded_key_len, [](const u8*, u16) {})) {
         if (!btree->removeLL(folded_key, folded_key_len)) {
            return false;
         }
      }
      return true;
   }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scan(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldRecord(folded_key, key);
      btree->scanAscLL(
          folded_key, folded_key_len,
          [&](u8* key, u8* payload, [[maybe_ununsed]] u16 key_length, [[maybe_unused]] u16 payload_length) {
             if (key_length != folded_key_len) {
                return false;
             }
             assert(payload_length == sizeof(Record));
             typename Record::Key typed_key;
             Record::unfoldRecord(key, typed_key);
             const Record& typed_payload = *reinterpret_cast<Record*>(payload);
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
      const bool found = btree->lookupOneLL(folded_key, folded_key_len, [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         assert(payload_length == sizeof(Record));
         Record& typed_payload = *const_cast<Record*>(reinterpret_cast<const Record*>(payload));
         local_f = (typed_payload).*f;
      });
      ensure(found);
      return local_f;
   }

   uint64_t count() { return btree->countEntries(); }
};
