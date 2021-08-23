#pragma once
#include "Adapter.hpp"
#include "Types.hpp"
// -------------------------------------------------------------------------------------
#include <wiredtiger.h>

#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
// -------------------------------------------------------------------------------------
#define error_check(p) \
   if (p)              \
   wiredtiger_strerror(p)
// -------------------------------------------------------------------------------------
// https://source.wiredtiger.com/10.0.0/config_strings.html
struct WiredTigerDB {
   WT_CONNECTION* conn;
   static thread_local WT_SESSION* session;
   static thread_local WT_CURSOR* cursor;

   WiredTigerDB()
   {
      std::string config_string("create, direct_io=[data, log, checkpoint], log=(enabled=false), session_max=2000, cache_size=" +
                                std::to_string(u64(FLAGS_dram_gib * 1024)) + "M");
      int ret = wiredtiger_open(FLAGS_ssd_path.c_str(), NULL, config_string.c_str(), &conn);
      error_check(ret);
   }
   void prepareThread()
   {
      // TODO: isolation level
      int ret = conn->open_session(conn, NULL, NULL, &session);
      error_check(ret);
      // -------------------------------------------------------------------------------------
      ret = session->create(session, "table:access", "key_format=S,value_format=S");  // ,type=lsm
      error_check(ret);
      // -------------------------------------------------------------------------------------
      ret = session->open_cursor(session, "table:access", NULL, "raw", &cursor);
      error_check(ret);
   }
   ~WiredTigerDB() { conn->close(conn, NULL); }
};
// -------------------------------------------------------------------------------------
// TODO: use separate table instead of single one with separators as long as we use the B-Tree implementation of WiredTiger
template <class Record>
struct WiredTigerAdapter : public Adapter<Record> {
   WiredTigerDB& map;
   using SEP = u32;  // use 32-bits integer as separator instead of column family

   WiredTigerAdapter(WiredTigerDB& map) : map(map) {}
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record) final
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item{folded_key, folded_key_len};
      WT_ITEM payload_item{&record, sizeof(record)};

      map.cursor->set_key(map.cursor, &key_item);
      map.cursor->set_value(map.cursor, &payload_item);
      int ret = map.cursor->insert(map.cursor);
      if (ret != 0)
         throw;
      map.cursor->reset(map.cursor);
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn) final
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item{folded_key, folded_key_len};
      WT_ITEM payload_item;
      map.cursor->set_key(map.cursor, &key_item);
      int ret = map.cursor->search(map.cursor);
      if (ret != 0)
         throw;
      ret = map.cursor->get_value(map.cursor, &payload_item);
      const Record& record = *reinterpret_cast<const Record*>(payload_item.data);
      fn(record);
      map.cursor->reset(map.cursor);
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
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item{folded_key, folded_key_len};

      map.cursor->set_key(map.cursor, &key_item);
      int ret = map.cursor->remove(map.cursor);
      map.cursor->reset(map.cursor);
      if (ret != 0)
         return false;
      return true;
   }
   // -------------------------------------------------------------------------------------
   template <class T>
   SEP getId(const T& str)
   {
      return __builtin_bswap32(*reinterpret_cast<const SEP*>(str.data)) ^ (1ul << 31);
   }
   // -------------------------------------------------------------------------------------
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& fn, std::function<void()>) final
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item = {folded_key, folded_key_len}, payload_item;
      map.cursor->set_key(map.cursor, &key_item);
      int exact;
      int ret = map.cursor->search_near(map.cursor, &exact);
      if (exact < 0)
         ret = map.cursor->next(map.cursor);
      while (ret == 0) {
         map.cursor->get_key(map.cursor, &key_item);
         map.cursor->get_value(map.cursor, &payload_item);
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(key_item.data + sizeof(SEP)), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(payload_item.data);
         if ((getId(key_item) != Record::id) || !fn(s_key, s_value))
            break;
         ret = map.cursor->next(map.cursor);
      }
      map.cursor->reset(map.cursor);
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                 std::function<void()>) final
   {
      u64 counter = 0;
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldKey(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      WT_ITEM key_item{folded_key, folded_key_len}, payload_item;
      map.cursor->set_key(map.cursor, &key_item);
      int exact;
      int ret = map.cursor->search_near(map.cursor, &exact);
      if (exact > 0)
         ret = map.cursor->prev(map.cursor);
      while (ret == 0) {
         counter++;
         map.cursor->get_key(map.cursor, &key_item);
         map.cursor->get_value(map.cursor, &payload_item);
         typename Record::Key s_key;
         Record::unfoldKey(reinterpret_cast<const u8*>(key_item.data + sizeof(SEP)), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(payload_item.data);
         if ((getId(key_item) != Record::id) || !fn(s_key, s_value))
            break;
         ret = map.cursor->prev(map.cursor);
      }
      map.cursor->reset(map.cursor);
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
      uint8_t keyStorage[sizeof(SEP)];
      fold(keyStorage, Record::id);
      WT_ITEM key{keyStorage, sizeof(SEP)};
      map.cursor->set_key(map.cursor, &key);
      int exact;
      int ret = map.cursor->search_near(map.cursor, &exact);
      if (exact < 0)
         ret = map.cursor->next(map.cursor);
      uint64_t count = 0;
      while (ret == 0) {
         map.cursor->get_key(map.cursor, &key);
         if (getId(key) != Record::id)
            break;
         count++;
         ret = map.cursor->next(map.cursor);
      }
      map.cursor->reset(map.cursor);
      return count;
   }
};
