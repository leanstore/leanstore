/**
 * @file adapter.hpp
 * @brief Standardizes working with storage engine
 * 
 */
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
      if (FLAGS_vw) {
         btree = &db.registerBTreeVW(name);
      } else if (FLAGS_vi) {
         btree = &db.registerBTreeVI(name);
      } else {
         btree = &db.registerBTreeLL(name);
      }
   }
   // -------------------------------------------------------------------------------------
   void printTreeHeight() { cout << name << " height = " << btree->getHeight() << endl; }
   // -------------------------------------------------------------------------------------
   // Functions for interaction with Leanstore
   // -------------------------------------------------------------------------------------

   /**
    * @brief Scans asc from start_key.
    *
    * @tparam Fn
    * @param key start_key
    * @param fn ??
    * @param undo ??
    */
   template <class Fn>
   void scan(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      btree->scanAsc(folded_key, folded_key_len, scanCallback(fn, folded_key_len), undo);
   }

   /**
    * @brief Scans desc from start_key.
    *
    * @tparam Fn
    * @param key start_key
    * @param fn ??
    * @param undo ??
    */
   template <class Fn>
   void scanDesc(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      btree->scanDesc(folded_key, folded_key_len, scanCallback(fn, folded_key_len), undo);
   }

   /**
    * @brief Insert record into storage.
    *
    * @param rec_key key of the record
    * @param record payload of the record
    */
   void insert(const typename Record::Key& rec_key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, rec_key);
      const auto res = btree->insert(folded_key, folded_key_len, (u8*)(&record), sizeof(Record));

      ensure(res == btree::OP_RESULT::OK || res == btree::OP_RESULT::ABORT_TX);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }

   /**
    * @brief Find one entry in storage.
    *
    * @tparam Fn
    * @param key Key for entry
    * @param fn Function to work with entry
    */
   template <class Fn>
   void lookup1(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const auto res = btree->lookup(folded_key, folded_key_len, lookup1Callback(fn));
      ensure(res == btree::OP_RESULT::OK);
   }

   /**
    * @brief Update one entry in storage.
    *
    * @tparam Fn
    * @param key Key for entry
    * @param fn ??
    * @param wal_update_generator ??
    */
   template <class Fn>
   void update1(const typename Record::Key& key, const Fn& fn, storage::btree::WALUpdateGenerator wal_update_generator)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const auto res = btree->updateSameSize(folded_key, folded_key_len, update1Callback(fn), wal_update_generator);
      ensure(res != btree::OP_RESULT::NOT_FOUND);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
   }

   /**
    * @brief Delete entry from storage.
    *
    * @param key Key for entry
    * @return true Success
    * @return false Failure
    */
   bool erase(const typename Record::Key& key)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);
      const auto res = btree->remove(folded_key, folded_key_len);
      if (res == btree::OP_RESULT::ABORT_TX) {
         cr::Worker::my().abortTX();
      }
      return (res == btree::OP_RESULT::OK);
   }

   /**
    * @brief Get one specific field from entry in storage.
    *
    * @tparam Field
    * @param key Key for entry
    * @param f Field to get
    * @return auto Value of field
    */
   template <class Field>
   auto lookupField(const typename Record::Key& key, Field Record::*f)
   {
      u8 folded_key[Record::maxFoldLength()];
      u16 folded_key_len = Record::foldKey(folded_key, key);

      Field local_f;
      const auto res = btree->lookup(folded_key, folded_key_len, lookupFieldCallback(f, local_f));
      ensure(res == btree::OP_RESULT::OK);
      return local_f;
   }

   // -------------------------------------------------------------------------------------
   // Callbackfunctions
   // -------------------------------------------------------------------------------------
   /**
    * @brief Callback for scan.
    *
    * @tparam Fn
    * @param fn Function to evaluate on element of b-tree
    * @param folded_key_len
    * @return function<bool(u8*, u16, u8*, u16)>
    */
   template <class Fn>
   function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> scanCallback(const Fn& fn, u16 folded_key_len)
   {
      return [&, folded_key_len](const u8* key, [[maybe_unused]] u16 key_length, const u8* payload, [[maybe_unused]] u16 payload_length) {
         if (key_length != folded_key_len) {
            return false;
         }
         static_cast<void>(payload_length);
         typename Record::Key typed_key;
         Record::unfoldKey(key, typed_key);
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         return fn(typed_key, typed_payload);
      };
   }

   /**
    * @brief Callback for lookup1.
    *
    * @tparam Fn
    * @param fn Does something with payload
    * @return function<void(const u8*, u16)>
    */
   template <class Fn>
   function<void(const u8*, u16)> lookup1Callback(const Fn& fn)
   {
      return [&](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         assert(payload_length == sizeof(Record));
         const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
         fn(typed_payload);
      };
   }

   /**
    * @brief Callback for update1.
    *
    * @tparam Fn
    * @param fn updates something in payload
    * @return function<void(u8*, u16)>
    */
   template <class Fn>
   function<void(u8*, u16)> update1Callback(const Fn& fn)
   {
      return [&](u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         assert(payload_length == sizeof(Record));
         Record& typed_payload = *reinterpret_cast<Record*>(payload);
         fn(typed_payload);
      };
   }

   /**
    * @brief Callback for lookupField
    *
    * @tparam Field
    * @param f field to lookup
    * @param local_f where to store field
    * @return function<void(const u8*, u16)>
    */
   template <class Field>
   function<void(const u8*, u16)> lookupFieldCallback(Field Record::*f, Field& local_f)
   {
      return [&, f](const u8* payload, u16 payload_length) {
         static_cast<void>(payload_length);
         assert(payload_length == sizeof(Record));
         Record& typed_payload = *const_cast<Record*>(reinterpret_cast<const Record*>(payload));
         local_f = (typed_payload).*f;
      };
   }
};
