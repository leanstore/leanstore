#pragma once

#include "benchmark/adapters/adapter.h"
#include "leanstore/kv_interface.h"
#include "leanstore/leanstore.h"

#include <typeindex>
#include <typeinfo>

template <class RecordBase>
struct LeanStoreAdapter : Adapter<RecordBase> {
 private:
  std::type_index relation_;
  leanstore::LeanStore *db_;
  leanstore::KVInterface *tree_;

  // helper for Scan operators
  void ScanImpl(const typename RecordBase::Key &r_key,
                const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb, bool scan_ascending);

 public:
  explicit LeanStoreAdapter(leanstore::LeanStore &db);
  ~LeanStoreAdapter() override = default;

  // -------------------------------------------------------------------------------------
  /** Misc APIs */
  void ToggleAppendBiasMode(bool append_bias);
  void SetComparisonOperator(leanstore::ComparisonOperator cmp);
  auto RelationSize() -> float;
  void MiniTransactionWrapper(const std::function<void()> &op, wid_t wid = 0);

  // -------------------------------------------------------------------------------------
  void Scan(const typename RecordBase::Key &key,
            const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) override;
  void ScanDesc(const typename RecordBase::Key &key,
                const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) override;
  void Insert(const typename RecordBase::Key &r_key, const RecordBase &record) override;
  void InsertRawPayload(const typename RecordBase::Key &r_key, std::span<const u8> record);
  void Update(const typename RecordBase::Key &r_key, const RecordBase &record) override;
  void UpdateRawPayload(const typename RecordBase::Key &r_key, std::span<const u8> record,
                        const typename Adapter<RecordBase>::AccessRecordFunc &fn);
  auto LookUp(const typename RecordBase::Key &r_key,
              const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool override;
  auto UpdateInPlace(const typename RecordBase::Key &r_key, const typename Adapter<RecordBase>::ModifyRecordFunc &fn,
                     FixedSizeDelta *delta = nullptr) -> bool override;
  auto Erase(const typename RecordBase::Key &r_key) -> bool override;
  auto Count() -> u64 override;

  // -------------------------------------------------------------------------------------
  auto RegisterBlob(std::span<u8> blob_payload, std::span<u8> prev_blob,
                    bool likely_grow) -> std::span<const u8> override;
  void LoadBlob(u8 *blob_handler, const std::function<void(std::span<const u8>)> &read_cb, bool partial_load) override;
  void RemoveBlob(u8 *blob_handler) override;
  auto LookUpBlob(std::span<uint8_t> blob_payload,
                  const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool override;
};

static_assert(GLOBAL_BLOCK_SIZE == leanstore::BLK_BLOCK_SIZE);
