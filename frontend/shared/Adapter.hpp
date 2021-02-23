/**
 * @file Adapter.hpp
 * @brief Standardized way of working with Storage Engines
 *
 */

#pragma once
#include "Types.hpp"
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

template <class Record>
class Adapter
{
  public:
   /**
    * @brief Scans asc.
    *
    * @tparam Fn
    * @param key start_key
    * @param fn Can read from record. Returns bool (continue scan).
    * @param undo Call if scan fails.
    */
   virtual void scan(const typename Record::Key& key,
                     const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                     std::function<void()> undo) = 0;
   /**
    * @brief Scans desc.
    *
    * @tparam Fn
    * @param key start_key
    * @param fn Can read from record. Returns bool (continue scan).
    * @param undo Call if scan fails.
    */
   virtual void scanDesc(const typename Record::Key& key,
                         const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                         std::function<void()> undo) = 0;
   /**
    * @brief Insert record into storage.
    *
    * @param rec_key key of the record
    * @param record payload of the record
    */
   virtual void insert(const typename Record::Key& key, const Record& record) = 0;
   /**
    * @brief Find one entry in storage.
    *
    * @tparam Fn
    * @param key Key for entry
    * @param fn Function to work with entry
    */
   virtual void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn) = 0;
   /**
    * @brief Update one entry in storage.
    *
    * @tparam Fn
    * @param key Key for entry
    * @param fn ??
    * @param wal_update_generator ??
    */
   virtual void update1(const typename Record::Key& key,
                        const std::function<void(Record&)>& fn,
                        leanstore::WALUpdateGenerator wal_update_generator) = 0;
   /**
    * @brief Delete entry from storage.
    *
    * @param key Key for entry
    * @return true Success
    * @return false Failure
    */
   virtual bool erase(const typename Record::Key& key) = 0;
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
      Field local_f;
      bool found = false;
      lookup1(key, [&](const Record record) {
         found = true;
         local_f = (record).*f;
      });
      ensure(found);
      return local_f;
   }

   virtual uint64_t count() = 0;
};
