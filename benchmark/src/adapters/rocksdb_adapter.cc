#include "benchmark/adapters/rocksdb_adapter.h"
#include "benchmark/tpcc/schema.h"
#include "benchmark/utils/test_utils.h"
#include "benchmark/ycsb/schema.h"

#include <iostream>

thread_local rocksdb::Transaction *RocksDB::txn = nullptr;

RocksDB::RocksDB(DB_TYPE type) : type(type) {
  wo.disableWAL = false;
  wo.sync       = false;
  // -------------------------------------------------------------------------------------
  rocksdb::Options db_options;
  db_options.use_direct_reads                       = true;
  db_options.use_direct_io_for_flush_and_compaction = true;
  db_options.db_write_buffer_size                   = 0;  // disabled
  // // db_options.write_buffer_size = 64 * 1024 * 1024; keep the default
  db_options.create_if_missing = true;
  db_options.manual_wal_flush  = true;
  db_options.compression       = rocksdb::CompressionType::kNoCompression;
  // // db_options.OptimizeLevelStyleCompaction(FLAGS_dram_gib * 1024 * 1024 * 1024);
  db_options.row_cache = rocksdb::NewLRUCache(FLAGS_bm_physical_gb * 1024 * 1024 * 1024);
  rocksdb::Status s;
  if (type == DB_TYPE::DB) {
    s = rocksdb::DB::Open(db_options, FLAGS_db_path, &db);
  } else if (type == DB_TYPE::TransactionDB) {
    s = rocksdb::TransactionDB::Open(db_options, {}, FLAGS_db_path, &tx_db);
  } else if (type == DB_TYPE::OptimisticDB) {
    s = rocksdb::OptimisticTransactionDB::Open(db_options, FLAGS_db_path, &optimistic_transaction_db);
  }
  if (!s.ok()) { std::cerr << s.ToString() << std::endl; }
  assert(s.ok());
}

RocksDB::~RocksDB() { delete db; }

void RocksDB::PrepareThread() {}

void RocksDB::StartTransaction([[maybe_unused]] bool si) {
  if (type == DB_TYPE::TransactionDB) {
    txn = tx_db->BeginTransaction(wo, {});
  } else if (type == DB_TYPE::OptimisticDB) {
    txn = optimistic_transaction_db->BeginTransaction({}, {});
  } else {
  }
}

void RocksDB::CommitTransaction() {
  if (type != DB_TYPE::DB) {
    auto s = txn->Commit();
    db->FlushWAL(true);
    delete txn;
    txn = nullptr;
  }
}

// -------------------------------------------------------------------------------------

/* RocksDB read utility */
template <typename T>
auto RSlice(T *ptr, uint64_t len) -> rocksdb::Slice {
  return rocksdb::Slice(reinterpret_cast<const char *>(ptr), len);
}

template <class T>
uint32_t GetId(const T &str) {
  return __builtin_bswap32(*reinterpret_cast<const uint32_t *>(str.data())) ^ (1ul << 31);
}

template <class RecordBase>
RocksDBAdapter<RecordBase>::RocksDBAdapter(RocksDB &map) : map_(map) {}

template <class RecordBase>
RocksDBAdapter<RecordBase>::~RocksDBAdapter() = default;

template <class RecordBase>
void RocksDBAdapter<RecordBase>::Scan(const typename RecordBase::Key &key,
                                      const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  uint8_t folded_key[RecordBase::MaxFoldLength() + sizeof(Separator)];
  const auto folded_key_len =
    Fold(folded_key, RecordBase::TYPE_ID) + RecordBase::FoldKey(folded_key + sizeof(Separator), key);
  // -------------------------------------------------------------------------------------
  auto *it = map_.db->NewIterator(map_.ro);
  typename RecordBase::Key s_key;
  for (it->Seek(RSlice(folded_key, folded_key_len)); it->Valid() && GetId(it->key()) == RecordBase::TYPE_ID;
       it->Next()) {
    RecordBase::UnfoldKey(reinterpret_cast<const uint8_t *>(it->key().data() + sizeof(Separator)), s_key);
    auto s_value = *reinterpret_cast<const RecordBase *>(it->value().data());
    if (!found_record_cb(s_key, s_value)) { break; }
  }
  assert(it->status().ok());
  delete it;
}

template <class RecordBase>
void RocksDBAdapter<RecordBase>::ScanDesc(const typename RecordBase::Key &key,
                                          const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  uint8_t folded_key[RecordBase::MaxFoldLength() + sizeof(Separator)];
  const auto folded_key_len =
    Fold(folded_key, RecordBase::TYPE_ID) + RecordBase::FoldKey(folded_key + sizeof(Separator), key);
  // -------------------------------------------------------------------------------------
  auto *it = map_.db->NewIterator(map_.ro);
  typename RecordBase::Key s_key;
  for (it->SeekForPrev(RSlice(folded_key, folded_key_len)); it->Valid() && GetId(it->key()) == RecordBase::TYPE_ID;
       it->Prev()) {
    RecordBase::UnfoldKey(reinterpret_cast<const uint8_t *>(it->key().data() + sizeof(Separator)), s_key);
    auto s_value = *reinterpret_cast<const RecordBase *>(it->value().data());
    if (!found_record_cb(s_key, s_value)) { break; }
  }
  assert(it->status().ok());
  delete it;
}

template <class RecordBase>
void RocksDBAdapter<RecordBase>::Insert(const typename RecordBase::Key &r_key, const RecordBase &record) {
  uint8_t folded_key[RecordBase::MaxFoldLength() + sizeof(Separator)];
  const auto folded_key_len =
    Fold(folded_key, RecordBase::TYPE_ID) + RecordBase::FoldKey(folded_key + sizeof(Separator), r_key);
  // -------------------------------------------------------------------------------------
  if (map_.type == RocksDB::DB_TYPE::DB) {
    auto s = map_.db->Put(map_.wo, RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
    Ensure(s.ok());
  } else {
    auto s = map_.txn->Put(RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
    if (!s.ok()) { map_.txn->Rollback(); }
  }
}

template <class RecordBase>
void RocksDBAdapter<RecordBase>::Update(const typename RecordBase::Key &r_key, const RecordBase &record) {
  [[maybe_unused]] int ret = Erase(r_key);
  assert(ret);
  Insert(r_key, record);
}

template <class RecordBase>
auto RocksDBAdapter<RecordBase>::LookUp(const typename RecordBase::Key &r_key,
                                        const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool {
  uint8_t folded_key[RecordBase::MaxFoldLength() + sizeof(Separator)];
  const auto folded_key_len =
    Fold(folded_key, RecordBase::TYPE_ID) + RecordBase::FoldKey(folded_key + sizeof(Separator), r_key);
  // -------------------------------------------------------------------------------------
  rocksdb::PinnableSlice value;
  rocksdb::Status s;
  if (map_.type == RocksDB::DB_TYPE::DB) {
    s = map_.db->Get(map_.ro, map_.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
  } else {
    s = map_.txn->Get(map_.ro, map_.db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
  }
  auto found = s.ok();
  if (found) {
    auto record = *reinterpret_cast<const RecordBase *>(value.data());
    fn(record);
    value.Reset();
  }
  return found;
}

template <class RecordBase>
auto RocksDBAdapter<RecordBase>::UpdateInPlace(const typename RecordBase::Key &r_key,
                                               const typename Adapter<RecordBase>::ModifyRecordFunc &fn,
                                               [[maybe_unused]] FixedSizeDelta *delta) -> bool {
  RecordBase r;
  auto found = LookUp(r_key, [&](const RecordBase &rec) { r = rec; });
  if (found) {
    fn(r);
    Insert(r_key, r);
  }
  return found;
}

template <class RecordBase>
auto RocksDBAdapter<RecordBase>::Erase(const typename RecordBase::Key &r_key) -> bool {
  uint8_t folded_key[RecordBase::MaxFoldLength() + sizeof(Separator)];
  const auto folded_key_len =
    Fold(folded_key, RecordBase::TYPE_ID) + RecordBase::FoldKey(folded_key + sizeof(Separator), r_key);
  // -------------------------------------------------------------------------------------
  rocksdb::Status s;
  if (map_.type == RocksDB::DB_TYPE::DB) {
    s = map_.db->Delete(map_.wo, RSlice(folded_key, folded_key_len));
    if (s.ok()) {
      return true;
    } else {
      return false;
    }
  } else {
    s = map_.txn->Delete(RSlice(folded_key, folded_key_len));
    if (!s.ok()) { map_.txn->Rollback(); }
    return true;
  }
}

template <class RecordBase>
auto RocksDBAdapter<RecordBase>::Count() -> uint64_t {
  uint64_t result = 0;
  auto it         = map_.db->NewIterator(map_.ro);
  for (it->SeekToFirst(); it->Valid(); it->Next()) { result++; }
  return result;
}

// For testing purpose
template class RocksDBAdapter<benchmark::RelationTest>;

// For TPC-C
template class RocksDBAdapter<tpcc::WarehouseType>;
template class RocksDBAdapter<tpcc::DistrictType>;
template class RocksDBAdapter<tpcc::CustomerType>;
template class RocksDBAdapter<tpcc::CustomerWDCType>;
template class RocksDBAdapter<tpcc::HistoryType>;
template class RocksDBAdapter<tpcc::NewOrderType>;
template class RocksDBAdapter<tpcc::OrderType>;
template class RocksDBAdapter<tpcc::OrderWDCType>;
template class RocksDBAdapter<tpcc::OrderLineType>;
template class RocksDBAdapter<tpcc::ItemType>;
template class RocksDBAdapter<tpcc::StockType>;

// For YCSB
template class RocksDBAdapter<ycsb::Relation<Varchar<120>, 0>>;
