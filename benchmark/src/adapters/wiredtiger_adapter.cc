#include "benchmark/adapters/wiredtiger_adapter.h"
#include "benchmark/tpcc/schema.h"
#include "benchmark/utils/test_utils.h"
#include "benchmark/ycsb/schema.h"

#include <cassert>
#include <csignal>
#include <iostream>

WiredTigerDB::WiredTigerDB() {
  std::string config_string(
    "create, direct_io=[data, log, checkpoint], log=(enabled), transaction_sync=(enabled=true,method=fsync), "
    "statistics_log=(wait=1), statistics=(all, clear), session_max=2000, eviction=(threads_max=4), cache_size=" +
    std::to_string(FLAGS_bm_physical_gb * 1024) + "M");
  remove(FLAGS_db_path.c_str());
  auto cmd = std::string("mkdir -p ") + FLAGS_db_path;
  int ret  = system(cmd.c_str());
  assert(ret == 0);
  ret = wiredtiger_open(FLAGS_db_path.c_str(), nullptr, config_string.c_str(), &conn);
  WTErrorCheck(ret);
}

WiredTigerDB::~WiredTigerDB() { conn->close(conn, nullptr); }

void WiredTigerDB::PrepareThread() {
  std::string session_config("isolation=");
  if (FLAGS_txn_default_isolation_level == "si") {
    session_config.append("snapshot");
  } else if (FLAGS_txn_default_isolation_level == "rc") {
    session_config.append("read-committed");
  } else {
    assert(FLAGS_txn_default_isolation_level == "ru");
    session_config.append("read-uncommitted");
  }
  int ret = conn->open_session(conn, nullptr, session_config.c_str(), &session);
  WTErrorCheck(ret);
}

void WiredTigerDB::StartTransaction(bool si) {
  if (si) {
    session->begin_transaction(session, "isolation=snapshot");
  } else {
    session->begin_transaction(session, "isolation=read-uncommitted");
  }
}

void WiredTigerDB::CommitTransaction() {
  auto to_commit = (Rand(FLAGS_worker_count) == 0) ? ",sync=on" : "";
  session->commit_transaction(session, to_commit);
}

void WiredTigerDB::CloseSession() { session->close(session, nullptr); }

// -------------------------------------------------------------------------------------
template <class RecordBase>
WiredTigerAdapter<RecordBase>::WiredTigerAdapter(WiredTigerDB &map) : map_(map) {
  relation_name_ = static_cast<std::string>("table:tree_" + std::to_string(RecordBase::TYPE_ID));
  int ret =
    map_.session->create(map_.session, relation_name_.c_str(), "key_format=S,value_format=S,memory_page_max=10M");
  WTErrorCheck(ret);
}

template <class RecordBase>
WiredTigerAdapter<RecordBase>::~WiredTigerAdapter() = default;

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::Scan(const typename RecordBase::Key &key,
                                         const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, key);
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret = map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr, "raw",
                                    &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
  // -------------------------------------------------------------------------------------
  WT_ITEM key_item;
  WT_ITEM payload_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;
  // -------------------------------------------------------------------------------------
  cursor->set_key(cursor, &key_item);
  int exact;
  ret = cursor->search_near(cursor, &exact);
  if (exact < 0) { ret = cursor->next(cursor); }

  while (ret == 0) {
    cursor->get_key(cursor, &key_item);
    cursor->get_value(cursor, &payload_item);
    typename RecordBase::Key s_key;
    RecordBase::UnfoldKey(reinterpret_cast<const uint8_t *>(key_item.data), s_key);
    const RecordBase &s_value = *reinterpret_cast<const RecordBase *>(payload_item.data);
    if (!found_record_cb(s_key, s_value)) { break; }
    ret = cursor->next(cursor);
  }
}

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::ScanDesc(const typename RecordBase::Key &key,
                                             const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, key);
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret = map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr, "raw",
                                    &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
  // -------------------------------------------------------------------------------------
  WT_ITEM key_item;
  WT_ITEM payload_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;
  // -------------------------------------------------------------------------------------
  cursor->set_key(cursor, &key_item);
  int exact;
  ret = cursor->search_near(cursor, &exact);
  if (exact > 0) { ret = cursor->prev(cursor); }
  while (ret == 0) {
    cursor->get_key(cursor, &key_item);
    cursor->get_value(cursor, &payload_item);
    typename RecordBase::Key s_key;
    RecordBase::UnfoldKey(reinterpret_cast<const uint8_t *>(key_item.data), s_key);
    const auto &s_value = *reinterpret_cast<const RecordBase *>(payload_item.data);
    if (!found_record_cb(s_key, s_value)) { break; }
    ret = cursor->prev(cursor);
  }
}

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::Insert(const typename RecordBase::Key &r_key, const RecordBase &record) {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, r_key);
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret = map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr, "raw",
                                    &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
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
  if (ret == WT_ROLLBACK) { WTErrorCheck(map_.session->rollback_transaction(map_.session, nullptr)); }
  WTErrorCheck(ret);
}

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::Update(const typename RecordBase::Key &r_key, const RecordBase &record) {
  [[maybe_unused]] int ret = Erase(r_key);
  Insert(r_key, record);
}

template <class RecordBase>
auto WiredTigerAdapter<RecordBase>::LookUp(const typename RecordBase::Key &r_key,
                                           const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, r_key);
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret = map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr, "raw",
                                    &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
  // -------------------------------------------------------------------------------------
  WT_ITEM key_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;
  WT_ITEM payload_item;
  // -------------------------------------------------------------------------------------
  cursor->set_key(cursor, &key_item);
  if (cursor->search(cursor)) { return false; }
  ret          = cursor->get_value(cursor, &payload_item);
  auto &record = *reinterpret_cast<const RecordBase *>(payload_item.data);
  fn(record);
  cursor->reset(cursor);
  return true;
}

template <class RecordBase>
auto WiredTigerAdapter<RecordBase>::UpdateInPlace(const typename RecordBase::Key &r_key,
                                                  const typename Adapter<RecordBase>::ModifyRecordFunc &fn,
                                                  [[maybe_unused]] FixedSizeDelta *delta) -> bool {
  RecordBase record;
  auto found = LookUp(r_key, [&](const RecordBase &rec) { record = rec; });
  if (found) {
    fn(record);
    Insert(r_key, record);
  }
  return found;
}

template <class RecordBase>
auto WiredTigerAdapter<RecordBase>::Erase(const typename RecordBase::Key &r_key) -> bool {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, r_key);
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret = map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr, "raw",
                                    &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
  // -------------------------------------------------------------------------------------
  WT_ITEM key_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;
  // -------------------------------------------------------------------------------------
  cursor->set_key(cursor, &key_item);
  ret = cursor->search(cursor);
  if (ret == 0) { ret = cursor->remove(cursor); }
  if (ret == WT_ROLLBACK) { WTErrorCheck(map_.session->rollback_transaction(map_.session, nullptr)); }
  return (ret == 0);
}

template <class RecordBase>
auto WiredTigerAdapter<RecordBase>::Count() -> uint64_t {
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret = map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr, "raw",
                                    &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
  cursor->reset(cursor);
  ret = cursor->next(cursor);
  // -------------------------------------------------------------------------------------
  uint64_t count = 0;
  while (ret == 0) {
    count++;
    ret = cursor->next(cursor);
  }
  return count;
}

// For testing purpose
template class WiredTigerAdapter<benchmark::RelationTest>;

// For TPC-C
template class WiredTigerAdapter<tpcc::WarehouseType>;
template class WiredTigerAdapter<tpcc::DistrictType>;
template class WiredTigerAdapter<tpcc::CustomerType>;
template class WiredTigerAdapter<tpcc::CustomerWDCType>;
template class WiredTigerAdapter<tpcc::HistoryType>;
template class WiredTigerAdapter<tpcc::NewOrderType>;
template class WiredTigerAdapter<tpcc::OrderType>;
template class WiredTigerAdapter<tpcc::OrderWDCType>;
template class WiredTigerAdapter<tpcc::OrderLineType>;
template class WiredTigerAdapter<tpcc::ItemType>;
template class WiredTigerAdapter<tpcc::StockType>;

// For YCSB
template class WiredTigerAdapter<ycsb::Relation<Varchar<120>, 0>>;
