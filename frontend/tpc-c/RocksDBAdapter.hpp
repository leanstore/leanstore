#pragma once
#include "Types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/storage/btree/core/BTreeInterface.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "rocksdb/db.h"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

struct RocksDB {
   rocksdb::DB* db = nullptr;
   rocksdb::WriteOptions wo;
   rocksdb::ReadOptions ro;

   RocksDB()
   {
      wo.disableWAL = true;
      wo.sync = false;
      // -------------------------------------------------------------------------------------
      rocksdb::Options db_options;
      db_options.use_direct_reads = true;
      db_options.use_direct_io_for_flush_and_compaction = true;
      db_options.db_write_buffer_size = FLAGS_dram_gib * 1024 * 1024 * 1024;
      db_options.write_buffer_size = db_options.db_write_buffer_size;
      db_options.max_bytes_for_level_base = db_options.db_write_buffer_size;
      db_options.create_if_missing = true;
      db_options.manual_wal_flush = true;
      db_options.compression = rocksdb::CompressionType::kNoCompression;
      db_options.OptimizeLevelStyleCompaction(4ull * 1024 * 1024 * 1024);
      db_options.row_cache = rocksdb::NewLRUCache(4ull * 1024 * 1024 * 1024);
      rocksdb::Status s = rocksdb::DB::Open(db_options, FLAGS_ssd_path, &db);
      if (!s.ok())
         cerr << s.ToString() << endl;
      assert(s.ok());
   }

   ~RocksDB() { delete db; }
};
// -------------------------------------------------------------------------------------
template <class Record>
struct RocksDBAdapter {
   using SEP = u32;  // use 32-bits integer as separator instead of column family
   RocksDB& map;
   RocksDBAdapter(RocksDB& map) : map(map) {}
   // -------------------------------------------------------------------------------------
   template <typename T>
   rocksdb::Slice RSlice(T* ptr, u64 len)
   {
      return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s = map.db->Put(map.wo, RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
      assert(s.ok());
   }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void lookup1(const typename Record::Key& key, const Fn& fn)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s = map.db->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      assert(s.ok());
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      fn(record);
      value.Reset();
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   auto lookupField(const typename Record::Key& key, Field Record::*f)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s = map.db->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      assert(s.ok());
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      auto tmp = record.*f;
      value.Reset();
      return tmp;
   }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void update1(const typename Record::Key& key, const Fn& fn, leanstore::storage::btree::WALUpdateGenerator)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      std::string value;
      rocksdb::Status s = map.db->Get(map.ro, map.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      assert(s.ok());
      Record& record = *reinterpret_cast<Record*>(value.data());
      fn(record);
      s = map.db->Put(map.wo, RSlice(folded_key, folded_key_len), RSlice(reinterpret_cast<const char*>(&record), sizeof(record)));
      assert(s.ok());
   }
   // -------------------------------------------------------------------------------------
   bool erase(const typename Record::Key& key)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s = map.db->Delete(map.wo, RSlice(folded_key, folded_key_len));
      if (s.ok()) {
         return true;
      } else {
         return false;
      }
   }
   // -------------------------------------------------------------------------------------
   template <class T>
   uint32_t getId(const T& str)
   {
      return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(str.data())) ^ (1ul << 31);
   }
   //             [&](const neworder_t::Key& key, const neworder_t&) {
   template <class Fn>
   void scan(const typename Record::Key& key, const Fn& fn, std::function<void()>)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = map.db->NewIterator(map.ro);
      for (it->Seek(RSlice(folded_key, folded_key_len)); it->Valid() && getId(it->key()) == Record::id; it->Next()) {
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!fn(s_key, s_value))
            break;
      }
      assert(it->status().ok());
      delete it;
   }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void scanDesc(const typename Record::Key& key, const Fn& fn, std::function<void()>)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = map.db->NewIterator(map.ro);
      for (it->SeekForPrev(RSlice(folded_key, folded_key_len)); it->Valid() && getId(it->key()) == Record::id; it->Prev()) {
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!fn(s_key, s_value))
            break;
      }
      assert(it->status().ok());
      delete it;
   }
};
