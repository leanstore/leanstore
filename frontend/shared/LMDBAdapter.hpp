#pragma once
#include "Adapter.hpp"
#include "Types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "lmdb++.hpp"  // Using C++ Wrapper from LMDB
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
// -------------------------------------------------------------------------------------
struct LMDB {
   lmdb::env env;
   lmdb::txn dummy_tx{nullptr};
   static thread_local lmdb::txn txn;

   LMDB() : env(lmdb::env::create())
   {
      env.set_max_dbs(100);
      // FLAGS_dram_gib is misued here to set the maximum map size for LMDB
      env.set_mapsize(FLAGS_dram_gib * 1024UL * 1024UL * 1024UL);
      env.open(FLAGS_ssd_path.c_str(), MDB_NOSYNC);
   }
   // -------------------------------------------------------------------------------------
   void startTX(bool read_only = false) { txn = lmdb::txn::begin(env, nullptr, read_only ? MDB_RDONLY : 0); }
   // -------------------------------------------------------------------------------------
   void commitTX() { txn.commit(); }
   // -------------------------------------------------------------------------------------
   void abortTX() { txn.abort(); }
   // -------------------------------------------------------------------------------------
   ~LMDB() {}
};
// -------------------------------------------------------------------------------------
template <class Record>
struct LMDBAdapter : public Adapter<Record> {
   LMDB& map;
   std::string name;
   lmdb::dbi dbi;
   // -------------------------------------------------------------------------------------
   lmdb::txn& hack()
   {
      map.dummy_tx = lmdb::txn::begin(map.env);
      return map.dummy_tx;
   }
   // -------------------------------------------------------------------------------------
   LMDBAdapter(LMDB& map, std::string name) : map(map), name(name), dbi(lmdb::dbi::open(hack(), name.c_str(), MDB_CREATE)) { map.dummy_tx.commit(); }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      lmdb::val lmdb_key{folded_key, folded_key_len};
      lmdb::val lmdb_payload{const_cast<Record*>(&record), sizeof(record)};
      // -------------------------------------------------------------------------------------
      if (!dbi.put(map.txn, lmdb_key, lmdb_payload))
         throw;
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      lmdb::val lmdb_key{folded_key, folded_key_len};
      lmdb::val lmdb_payload;
      // -------------------------------------------------------------------------------------
      if (!dbi.get(map.txn, lmdb_key, lmdb_payload))
         throw;
      Record& record = *reinterpret_cast<Record*>(lmdb_payload.data());
      fn(record);
   }
   // -------------------------------------------------------------------------------------
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& fn, leanstore::UpdateSameSizeInPlaceDescriptor&) final
   {
      Record r;
      lookup1(key, [&](const Record& rec) { r = rec; });
      fn(r);
      insert(key, r);
   }
   // -------------------------------------------------------------------------------------
   bool erase(const typename Record::Key& key) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      lmdb::val lmdb_key{folded_key, folded_key_len};
      // -------------------------------------------------------------------------------------
      if (!dbi.del(map.txn, lmdb_key))
         throw;
      return true;
   }
   // -------------------------------------------------------------------------------------
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& fn, std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      lmdb::val lmdb_key{folded_key, folded_key_len};
      // -------------------------------------------------------------------------------------
      lmdb::val lmdb_payload;
      lmdb::cursor cursor = lmdb::cursor::open(map.txn, dbi);
      if (cursor.get(lmdb_key, lmdb_payload, MDB_SET_RANGE)) {
         bool cont;
         do {
            typename Record::Key s_key;
            Record::unfoldKey(reinterpret_cast<const u8*>(lmdb_key.data()), s_key);
            Record& s_value = *reinterpret_cast<Record*>(lmdb_payload.data());
            cont = fn(s_key, s_value);
         } while (cont && cursor.get(lmdb_key, lmdb_payload, MDB_NEXT));
      }
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                 std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      lmdb::val lmdb_key{folded_key, folded_key_len};
      // -------------------------------------------------------------------------------------
      lmdb::val lmdb_payload;
      lmdb::cursor cursor = lmdb::cursor::open(map.txn, dbi);
      if (!cursor.get(lmdb_key, lmdb_payload, MDB_SET_RANGE)) {
         if (!cursor.get(lmdb_key, lmdb_payload, MDB_LAST)) {
            return;
         }
      }
      while (true) {
         std::basic_string_view<u8> upper(folded_key, folded_key_len);
         std::basic_string_view<u8> current(reinterpret_cast<u8*>(lmdb_key.data()), lmdb_key.size());
         if (current > upper) {
            if (cursor.get(lmdb_key, lmdb_payload, MDB_PREV)) {
               continue;
            } else {
               return;
            }
         } else {
            break;
         }
      }
      bool cont;
      do {
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(lmdb_key.data()), s_key);
         Record& s_value = *reinterpret_cast<Record*>(lmdb_payload.data());
         cont = fn(s_key, s_value);
      } while (cont && cursor.get(lmdb_key, lmdb_payload, MDB_PREV));
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   auto lookupField(const typename Record::Key& key, Field f)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, key);
      lmdb::val lmdb_key{folded_key, folded_key_len};
      lmdb::val lmdb_payload;
      // -------------------------------------------------------------------------------------
      if (!dbi.get(map.txn, lmdb_key, lmdb_payload))
         throw;
      Record& record = *reinterpret_cast<Record*>(lmdb_payload.data());
      auto ret = record.*f;
      return ret;
   }
   // -------------------------------------------------------------------------------------
   uint64_t count()
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldKey(folded_key, 0);
      lmdb::val lmdb_key{folded_key, folded_key_len};
      lmdb::cursor cursor = lmdb::cursor::open(map.txn, dbi);
      lmdb::val lmdb_payload;
      // -------------------------------------------------------------------------------------
      uint64_t count = 0;
      if (cursor.get(lmdb_key, lmdb_payload, MDB_SET_RANGE)) {
         do {
            count++;
         } while (cursor.get(lmdb_key, lmdb_payload, MDB_NEXT));
      }
      return count;
   }
};
