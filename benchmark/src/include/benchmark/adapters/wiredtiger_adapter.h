#pragma once

#include "benchmark/adapters/adapter.h"

#include "share_headers/config.h"
#include "wiredtiger.h"

#define WTErrorCheck(p)                               \
  if (p) {                                            \
    std::cerr << wiredtiger_strerror(p) << std::endl; \
    raise(SIGTRAP);                                   \
  }

struct WiredTigerDB {
  WT_CONNECTION *conn;
  inline static thread_local WT_SESSION *session    = nullptr;
  inline static thread_local WT_CURSOR *cursors[20] = {nullptr};

  WiredTigerDB();
  ~WiredTigerDB();

  void PrepareThread();
  void StartTransaction(bool si = true);
  void CommitTransaction();
  void CloseSession();
};

template <class RecordBase>
class WiredTigerAdapter : Adapter<RecordBase> {
 private:
  WiredTigerDB &map_;
  std::string relation_name_;

 public:
  explicit WiredTigerAdapter(WiredTigerDB &map);
  ~WiredTigerAdapter() override;

  // -------------------------------------------------------------------------------------
  void Scan(const typename RecordBase::Key &key,
            const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) override;
  void ScanDesc(const typename RecordBase::Key &key,
                const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) override;
  void Insert(const typename RecordBase::Key &r_key, const RecordBase &record) override;
  void Update(const typename RecordBase::Key &r_key, const RecordBase &record) override;
  auto LookUp(const typename RecordBase::Key &r_key, const typename Adapter<RecordBase>::AccessRecordFunc &fn)
    -> bool override;
  void UpdateInPlace(const typename RecordBase::Key &r_key,
                     const typename Adapter<RecordBase>::ModifyRecordFunc &fn) override;
  auto Erase(const typename RecordBase::Key &r_key) -> bool override;

  // -------------------------------------------------------------------------------------
  auto Count() -> uint64_t override;
};