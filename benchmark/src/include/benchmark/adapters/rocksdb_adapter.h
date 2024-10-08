#pragma once

#include "benchmark/adapters/adapter.h"

#include "rocksdb/db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "share_headers/config.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

struct RocksDB : BaseDatabase {
  union {
    rocksdb::DB *db = nullptr;
    rocksdb::TransactionDB *tx_db;
    rocksdb::OptimisticTransactionDB *optimistic_transaction_db;
  };

  static thread_local rocksdb::Transaction *txn;
  rocksdb::WriteOptions wo;
  rocksdb::ReadOptions ro;

  enum class DB_TYPE : uint8_t { DB, TransactionDB, OptimisticDB };
  const DB_TYPE type;

  explicit RocksDB(DB_TYPE type = DB_TYPE::DB);
  ~RocksDB();

  void PrepareThread();
  void StartTransaction(bool si = true);
  void CommitTransaction();
};

template <class RecordBase>
class RocksDBAdapter : public Adapter<RecordBase> {
 private:
  using Separator = uint32_t;  // use 32-bits integer as separator instead of column family

  RocksDB &map_;
  std::string relation_name_;

 public:
  explicit RocksDBAdapter(RocksDB &map);
  ~RocksDBAdapter() override;

  // -------------------------------------------------------------------------------------
  void Scan(const typename RecordBase::Key &key,
            const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) override;
  void ScanDesc(const typename RecordBase::Key &key,
                const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) override;
  void Insert(const typename RecordBase::Key &r_key, const RecordBase &record) override;
  void Update(const typename RecordBase::Key &r_key, const RecordBase &record) override;
  auto LookUp(const typename RecordBase::Key &r_key,
              const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool override;
  auto UpdateInPlace(const typename RecordBase::Key &r_key, const typename Adapter<RecordBase>::ModifyRecordFunc &fn,
                     FixedSizeDelta *delta = nullptr) -> bool override;
  auto Erase(const typename RecordBase::Key &r_key) -> bool override;

  // -------------------------------------------------------------------------------------
  auto Count() -> uint64_t override;
};