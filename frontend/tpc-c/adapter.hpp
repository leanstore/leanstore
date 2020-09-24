#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>

#include "leanstore/LeanStore.hpp"
#include "types.hpp"
using namespace leanstore;
template <class Record>
struct LeanStoreAdapter {
  btree::vs::BTree* btree;
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
  template <class T>
  static std::string getStringKey(const T& key)
  {
    uint8_t foldKey[Record::maxFoldLength()];
    unsigned foldKeyLen = Record::foldRecord(foldKey, key);
    return std::string(reinterpret_cast<char*>(foldKey), foldKeyLen);
  }
  // -------------------------------------------------------------------------------------
  // key_length - truncate_from_end  gives us the length of the prefix
  // it gives us the maximum tuple with this prefix
  template <class Fn>
  void prefixMax1(const typename Record::Key& key, const u64 truncate_from_end, const Fn& fn)
  {
    string key_str = getStringKey(key);
    const bool found =
        btree->prefixMaxOne((u8*)key_str.data(), key_str.length() - truncate_from_end, [&](const u8* key, const u8* payload, u16 payload_length) {
          static_cast<void>(payload_length);
          typename Record::Key typed_key;
          Record::unfoldRecord(key, typed_key);
          const Record& typed_payload = *reinterpret_cast<const Record*>(payload);
          assert(payload_length == sizeof(Record));
          fn(typed_key, typed_payload);
        });
    if (!found) {
      ensure(false);
    }
  }
  // -------------------------------------------------------------------------------------
  template <class Fn>
  void scanDesc(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
  {
    string key_str = getStringKey(key);
    btree->rangeScanDesc(
        reinterpret_cast<u8*>(key_str.data()), u16(key_str.length()),
        [&](u8* key, u8* payload, [[maybe_unused]] u16 payload_length) {
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
    string key = getStringKey(rec_key);
    btree->insert((u8*)key.data(), key.length(), sizeof(Record), (u8*)(&record));
  }

  template <class Fn>
  void lookup1(const typename Record::Key& key, const Fn& fn)
  {
    string key_str = getStringKey(key);
    const bool found = btree->lookupOne((u8*)key_str.data(), key_str.length(), [&](const u8* payload, u16 payload_length) {
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
  void update1(const typename Record::Key& key, const Fn& fn)
  {
    string key_str = getStringKey(key);
    btree->updateSameSize((u8*)key_str.data(), key_str.length(), [&](u8* payload, u16 payload_length) {
      static_cast<void>(payload_length);
      assert(payload_length == sizeof(Record));
      Record& typed_payload = *reinterpret_cast<Record*>(payload);
      fn(typed_payload);
    });
  }

  bool erase(const typename Record::Key& key)
  {
    string key_str = getStringKey(key);
    if (btree->lookupOne((u8*)key_str.data(), key_str.length(), [](const u8*, u16) {})) {
      if (!btree->remove((u8*)key_str.data(), key_str.length())) {
        return false;
      }
    }
    return true;
  }
  // -------------------------------------------------------------------------------------
  template <class Fn>
  void scan(const typename Record::Key& key, const Fn& fn, std::function<void()> undo)
  {
    string key_str = getStringKey(key);
    btree->rangeScanAsc(
        reinterpret_cast<u8*>(key_str.data()), u16(key_str.length()),
        [&](u8* key, u8* payload, [[maybe_unused]] u16 payload_length) {
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
    string key_str = getStringKey(key);
    Field local_f;
    const bool found = btree->lookupOne((u8*)key_str.data(), key_str.length(), [&](const u8* payload, u16 payload_length) {
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
