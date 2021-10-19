#pragma once
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include <wiredtiger.h>

#include "leanstore/LeanStore.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
// -------------------------------------------------------------------------------------
#define error_check(p)                        \
   if (p) {                                   \
      cerr << wiredtiger_strerror(p) << endl; \
      raise(SIGTRAP);                         \
   }
// -------------------------------------------------------------------------------------
// https://source.wiredtiger.com/10.0.0/config_strings.html
struct WiredTigerDB {
   WT_CONNECTION* conn;
   static thread_local WT_SESSION* session;
   static thread_local WT_CURSOR* cursor[20];

   WiredTigerDB()
   {
      std::string config_string("create, direct_io=[data, log, checkpoint], log=(enabled=false), session_max=2000, cache_size=" +
                                std::to_string(u64(FLAGS_dram_gib * 1024)) + "M");
      int ret = wiredtiger_open(FLAGS_ssd_path.c_str(), NULL, config_string.c_str(), &conn);
      error_check(ret);
   }
   void prepareThread()
   {
      std::string session_config("isolation=snapshot");
      int ret = conn->open_session(conn, NULL, session_config.c_str(), &session);
      error_check(ret);
   }
   void startTX() { session->begin_transaction(session, NULL); }
   void commitTX() { session->commit_transaction(session, NULL); }
   ~WiredTigerDB() { conn->close(conn, NULL); }
};
// -------------------------------------------------------------------------------------
// TODO: use separate table instead of single one with separators as long as we use the B-Tree implementation of WiredTiger
template <class Record>
struct WiredTigerAdapter {
   WiredTigerDB* map;
   std::string table_name;
   WiredTigerAdapter() : map(nullptr) {}  // Hack
   WiredTigerAdapter(WiredTigerDB& db) : map(&db)
   {
      table_name = std::string("table:tree_" + std::to_string(Record::id));
      int ret = map->session->create(map->session, table_name.c_str(), "key_format=S,value_format=S");  // ,type=lsm
      error_check(ret);
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldRecord(folded_key, key);
      int ret;
      // -------------------------------------------------------------------------------------
      if (map->cursor[Record::id] == nullptr) {
         ret = map->session->open_cursor(map->session, table_name.c_str(), NULL, "raw", &map->cursor[Record::id]);
         error_check(ret);
      }
      WT_CURSOR* cursor = map->cursor[Record::id];
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item;
      key_item.data = folded_key;
      key_item.size = folded_key_len;
      WT_ITEM payload_item;
      payload_item.data = &record;
      payload_item.size = sizeof(record);
      // -------------------------------------------------------------------------------------
      cursor->set_key(cursor, &key_item);
      cursor->set_value(cursor, &payload_item);
      ret = cursor->insert(cursor);
      if (ret == WT_ROLLBACK) {
         error_check(map->session->rollback_transaction(map->session, NULL));
         jumpmu::jump();
      }
      error_check(ret);
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldRecord(folded_key, key);
      int ret;
      // -------------------------------------------------------------------------------------
      if (map->cursor[Record::id] == nullptr) {
         ret = map->session->open_cursor(map->session, table_name.c_str(), NULL, "raw", &map->cursor[Record::id]);
         error_check(ret);
      }
      WT_CURSOR* cursor = map->cursor[Record::id];
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item;
      key_item.data = folded_key;
      key_item.size = folded_key_len;
      WT_ITEM payload_item;
      // -------------------------------------------------------------------------------------
      cursor->set_key(cursor, &key_item);
      ret = cursor->search(cursor);
      error_check(ret);
      ret = cursor->get_value(cursor, &payload_item);
      error_check(ret);
      const Record& record = *reinterpret_cast<const Record*>(payload_item.data);
      fn(record);
      cursor->reset(cursor);
   }
   // -------------------------------------------------------------------------------------
   template <class Fn>
   void update1(const typename Record::Key& key, const Fn& fn, leanstore::storage::btree::WALUpdateGenerator)
   {
      Record r;
      lookup1(key, [&](const Record& rec) { r = rec; });
      fn(r);
      insert(key, r);
   }
   // -------------------------------------------------------------------------------------
   bool erase(const typename Record::Key& key)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldRecord(folded_key, key);
      int ret;
      // -------------------------------------------------------------------------------------
      if (map->cursor[Record::id] == nullptr) {
         ret = map->session->open_cursor(map->session, table_name.c_str(), NULL, "raw", &map->cursor[Record::id]);
         error_check(ret);
      }
      WT_CURSOR* cursor = map->cursor[Record::id];
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item;
      key_item.data = folded_key;
      key_item.size = folded_key_len;
      // -------------------------------------------------------------------------------------
      cursor->set_key(cursor, &key_item);
      ret = cursor->remove(cursor);
      if (ret == WT_ROLLBACK) {
         error_check(map->session->rollback_transaction(map->session, NULL));
         jumpmu::jump();
      }
      return (ret == 0);
   }
   // -------------------------------------------------------------------------------------
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& fn, std::function<void()>)
   {
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldRecord(folded_key, key);
      int ret;
      // -------------------------------------------------------------------------------------
      if (map->cursor[Record::id] == nullptr) {
         ret = map->session->open_cursor(map->session, table_name.c_str(), NULL, "raw", &map->cursor[Record::id]);
         error_check(ret);
      }
      WT_CURSOR* cursor = map->cursor[Record::id];
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item, payload_item;
      key_item.data = folded_key;
      key_item.size = folded_key_len;
      // -------------------------------------------------------------------------------------
      cursor->set_key(cursor, &key_item);
      int exact;
      ret = cursor->search_near(cursor, &exact);
      if (exact < 0)
         ret = cursor->next(cursor);
      while (ret == 0) {
         cursor->get_key(cursor, &key_item);
         cursor->get_value(cursor, &payload_item);
         typename Record::Key s_key;
         Record::unfoldRecord(reinterpret_cast<const u8*>(key_item.data), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(payload_item.data);
         if (!fn(s_key, s_value))
            break;
         ret = cursor->next(cursor);
      }
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& fn, std::function<void()>)
   {
      u64 counter = 0;
      u8 folded_key[Record::maxFoldLength()];
      const u32 folded_key_len = Record::foldRecord(folded_key, key);
      int ret;
      // -------------------------------------------------------------------------------------
      if (map->cursor[Record::id] == nullptr) {
         ret = map->session->open_cursor(map->session, table_name.c_str(), NULL, "raw", &map->cursor[Record::id]);
         error_check(ret);
      }
      WT_CURSOR* cursor = map->cursor[Record::id];
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item, payload_item;
      key_item.data = folded_key;
      key_item.size = folded_key_len;
      // -------------------------------------------------------------------------------------
      cursor->set_key(cursor, &key_item);
      int exact;
      ret = cursor->search_near(cursor, &exact);
      if (exact > 0)
         ret = cursor->prev(cursor);
      while (ret == 0) {
         counter++;
         cursor->get_key(cursor, &key_item);
         cursor->get_value(cursor, &payload_item);
         typename Record::Key s_key;
         Record::unfoldRecord(reinterpret_cast<const u8*>(key_item.data), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(payload_item.data);
         if (!fn(s_key, s_value))
            break;
         ret = cursor->prev(cursor);
      }
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f)
   {
      Field local_f;
      bool found = false;
      lookup1(key, [&](const Record& record) {
         found = true;
         local_f = (record).*f;
      });
      assert(found);
      return local_f;
   }
   // -------------------------------------------------------------------------------------
   uint64_t count()
   {
      int ret;
      // -------------------------------------------------------------------------------------
      if (map->cursor[Record::id] == nullptr) {
         ret = map->session->open_cursor(map->session, table_name.c_str(), NULL, "raw", &map->cursor[Record::id]);
         error_check(ret);
      }
      WT_CURSOR* cursor = map->cursor[Record::id];
      // -------------------------------------------------------------------------------------
      WT_ITEM key;
      key.data = "";
      key.size = 0;
      // -------------------------------------------------------------------------------------
      cursor->set_key(cursor, &key);
      int exact;
      ret = cursor->search_near(cursor, &exact);
      if (exact < 0)
         ret = cursor->next(cursor);
      uint64_t count = 0;
      while (ret == 0) {
         cursor->get_key(cursor, &key);
         count++;
         ret = cursor->next(cursor);
      }
      return count;
   }
};
