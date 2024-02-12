#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/filebench/webserver/schema.h"
#include "benchmark/fuse/schema.h"
#include "benchmark/gitclone/schema.h"
#include "benchmark/tpcc/schema.h"
#include "benchmark/utils/test_utils.h"
#include "benchmark/wikipedia/schema.h"
#include "benchmark/ycsb/schema.h"
#include "leanstore/external_schema.h"

#include <span>

template <class RecordBase>
LeanStoreAdapter<RecordBase>::LeanStoreAdapter(leanstore::LeanStore &db)
    : relation_(static_cast<std::type_index>(typeid(RecordBase))), db_(&db) {
  db_->RegisterTable(relation_);
  tree_ = db_->RetrieveIndex(relation_);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::ToggleAppendBiasMode(bool append_bias) {
  tree_->ToggleAppendBiasMode(append_bias);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::SetComparisonOperator(leanstore::ComparisonOperator cmp) {
  tree_->SetComparisonOperator(db_->RetrieveComparisonFunc(cmp));
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::ScanImpl(const typename RecordBase::Key &r_key,
                                            const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb,
                                            bool scan_ascending) {
  u8 key[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(key, r_key);

  auto read_cb = [&](std::span<u8> key, std::span<u8> payload) -> bool {
    typename RecordBase::Key typed_key;
    RecordBase::UnfoldKey(key.data(), typed_key);
    return found_record_cb(typed_key, *reinterpret_cast<const RecordBase *>(payload.data()));
  };

  if (scan_ascending) {
    tree_->ScanAscending({key, len}, read_cb);
  } else {
    tree_->ScanDescending({key, len}, read_cb);
  }
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::Scan(const typename RecordBase::Key &key,
                                        const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  ScanImpl(key, found_record_cb, true);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::ScanDesc(const typename RecordBase::Key &key,
                                            const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  ScanImpl(key, found_record_cb, false);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::Insert(const typename RecordBase::Key &r_key, const RecordBase &record) {
  u8 key[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(key, r_key);
  tree_->Insert({key, len}, {reinterpret_cast<const u8 *>(&record), record.PayloadSize()});
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::InsertRawPayload(const typename RecordBase::Key &r_key, std::span<const u8> record) {
  Ensure(record.size() == reinterpret_cast<const RecordBase *>(record.data())->PayloadSize());
  u8 key[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(key, r_key);
  tree_->Insert({key, len}, record);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::Update(const typename RecordBase::Key &r_key, const RecordBase &record) {
  u8 key[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(key, r_key);
  tree_->Update({key, len}, {reinterpret_cast<const u8 *>(&record), record.PayloadSize()}, {});
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::UpdateRawPayload(const typename RecordBase::Key &r_key, std::span<const u8> record,
                                                    const typename Adapter<RecordBase>::AccessRecordFunc &fn) {
  Ensure(record.size() == reinterpret_cast<const RecordBase *>(record.data())->PayloadSize());
  u8 key[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(key, r_key);
  tree_->Update({key, len}, record,
                [&](std::span<u8> payload) { fn(*reinterpret_cast<RecordBase *>(payload.data())); });
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::LookUp(const typename RecordBase::Key &r_key,
                                          const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool {
  u8 key[RecordBase::MaxFoldLength()];
  auto len     = RecordBase::FoldKey(key, r_key);
  bool success = tree_->LookUp({key, len}, [&](std::span<u8> pl) { fn(*reinterpret_cast<RecordBase *>(pl.data())); });
  return success;
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::UpdateInPlace(const typename RecordBase::Key &r_key,
                                                 const typename Adapter<RecordBase>::ModifyRecordFunc &fn) {
  u8 key[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(key, r_key);
  tree_->UpdateInPlace({key, len}, [&](std::span<u8> payload) { fn(*reinterpret_cast<RecordBase *>(payload.data())); });
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::Erase(const typename RecordBase::Key &r_key) -> bool {
  u8 key[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(key, r_key);
  return tree_->Remove({key, len});
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::Count() -> u64 {
  return tree_->CountEntries();
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::RelationSize() -> float {
  return tree_->SizeInMB();
}

/**
 * @brief Tiny wrapper for transaction execution, help with code reusability
 */
template <class RecordBase>
void LeanStoreAdapter<RecordBase>::MiniTransactionWrapper(const std::function<void()> &op, wid_t wid) {
  db_->worker_pool.ScheduleSyncJob(wid, [&]() {
    db_->StartTransaction();
    op();
    db_->CommitTransaction();
  });
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::RegisterBlob(std::span<u8> blob_payload, std::span<u8> prev_blob, bool likely_grow)
  -> std::span<const u8> {
  auto prev_btup = (prev_blob.empty()) ? nullptr : reinterpret_cast<leanstore::BlobState *>(prev_blob.data());
  return db_->CreateNewBlob(blob_payload, prev_btup, likely_grow);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::LoadBlob(u8 *blob_handler, const std::function<void(std::span<const u8>)> &read_cb,
                                            bool partial_load) {
  auto blob = reinterpret_cast<leanstore::BlobState *>(blob_handler);
  Ensure((leanstore::BlobState::MIN_MALLOC_SIZE <= blob->MallocSize()) &&
         (leanstore::BlobState::MAX_MALLOC_SIZE >= blob->MallocSize()));
  db_->LoadBlob(blob, read_cb, partial_load);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::RemoveBlob(u8 *blob_handler) {
  auto blob = reinterpret_cast<leanstore::BlobState *>(blob_handler);
  db_->RemoveBlob(blob);
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::LookUpBlob(std::span<u8> blob_payload,
                                              const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool {
  auto success =
    tree_->LookUpBlob(blob_payload, db_->RetrieveComparisonFunc(leanstore::ComparisonOperator::BLOB_LOOKUP),
                      [&](std::span<u8> payload) { fn(*reinterpret_cast<const RecordBase *>(payload.data())); });
  return success;
}

// Misc
template struct LeanStoreAdapter<benchmark::RelationTest>;
template struct LeanStoreAdapter<benchmark::VariableSizeRelation>;
template struct LeanStoreAdapter<leanstore::fuse::FileRelation>;

// For TPC-C
template struct LeanStoreAdapter<tpcc::WarehouseType>;
template struct LeanStoreAdapter<tpcc::DistrictType>;
template struct LeanStoreAdapter<tpcc::CustomerType>;
template struct LeanStoreAdapter<tpcc::CustomerWDCType>;
template struct LeanStoreAdapter<tpcc::HistoryType>;
template struct LeanStoreAdapter<tpcc::NewOrderType>;
template struct LeanStoreAdapter<tpcc::OrderType>;
template struct LeanStoreAdapter<tpcc::OrderWDCType>;
template struct LeanStoreAdapter<tpcc::OrderLineType>;
template struct LeanStoreAdapter<tpcc::ItemType>;
template struct LeanStoreAdapter<tpcc::StockType>;

// For YCSB
template struct LeanStoreAdapter<ycsb::Relation<BytesPayload<120>, 0>>;
template struct LeanStoreAdapter<ycsb::BlobStateRelation>;

// For FileBench - WebServer
template struct LeanStoreAdapter<filebench::webserver::BlobStateRelation<0>>;
template struct LeanStoreAdapter<filebench::webserver::BlobStateRelation<1>>;

// For Wikipedia/Git Clone workloads
template struct LeanStoreAdapter<wiki::BlobStateIndex>;
template struct LeanStoreAdapter<wiki::BlobPrefixIndex>;
template struct LeanStoreAdapter<leanstore::schema::BlobRelation<0>>;
template struct LeanStoreAdapter<leanstore::schema::InrowBlobRelation<0>>;
template struct LeanStoreAdapter<leanstore::schema::InrowBlobRelation<1>>;
template struct LeanStoreAdapter<leanstore::schema::InrowBlobRelation<2>>;
