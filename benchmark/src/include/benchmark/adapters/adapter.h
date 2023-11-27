#pragma once

#include "share_headers/db_types.h"

#include <functional>
#include <span>

static constexpr uint64_t GLOBAL_BLOCK_SIZE = 4096;

// Unified interface used for different storage engines including LeanStore
template <class RecordBase>
class Adapter {
 public:
  virtual ~Adapter() = default;

  using FoundRecordFunc  = std::function<bool(const typename RecordBase::Key &, const RecordBase &)>;
  using AccessRecordFunc = std::function<void(const RecordBase &)>;
  using ModifyRecordFunc = std::function<void(RecordBase &)>;

  // -------------------------------------------------------------------------------------
  virtual void Scan(const typename RecordBase::Key &key,
                    const Adapter<RecordBase>::FoundRecordFunc &found_record_cb) = 0;
  // -------------------------------------------------------------------------------------
  virtual void ScanDesc(const typename RecordBase::Key &key,
                        const Adapter<RecordBase>::FoundRecordFunc &found_record_cb) = 0;
  // -------------------------------------------------------------------------------------
  virtual void Insert(const typename RecordBase::Key &key, const RecordBase &record) = 0;
  // -------------------------------------------------------------------------------------
  virtual void Update(const typename RecordBase::Key &key, const RecordBase &record) = 0;
  // -------------------------------------------------------------------------------------
  virtual auto LookUp(const typename RecordBase::Key &key, const Adapter<RecordBase>::AccessRecordFunc &callback)
    -> bool = 0;
  // -------------------------------------------------------------------------------------
  virtual void UpdateInPlace(const typename RecordBase::Key &key, const Adapter<RecordBase>::ModifyRecordFunc &fn) = 0;
  // -------------------------------------------------------------------------------------
  // Returns false if the record was not found
  virtual auto Erase(const typename RecordBase::Key &key) -> bool = 0;

  // -------------------------------------------------------------------------------------
  template <class Field>
  auto LookupField(const typename RecordBase::Key &key, Field RecordBase::*f) -> Field {
    Field value;
    LookUp(key, [&](const RecordBase &r) { value = r.*f; });
    return value;
  }

  // -------------------------------------------------------------------------------------
  /**
   * @brief Statistic APIs
   */
  virtual auto Count() -> uint64_t = 0;

  // -------------------------------------------------------------------------------------
  /**
   * @brief Expose BLOB APIs to the world
   */
  virtual auto RegisterBlob([[maybe_unused]] std::span<uint8_t> blob_payload,
                            [[maybe_unused]] std::span<uint8_t> prev_blob_rep, [[maybe_unused]] bool likely_grow)
    -> std::span<const uint8_t> {
    return {};
  }

  virtual void LoadBlob([[maybe_unused]] uint8_t *blob_rep,
                        [[maybe_unused]] const std::function<void(std::span<const uint8_t>)> &read_cb,
                        [[maybe_unused]] bool partial_load) {}

  virtual void RemoveBlob([[maybe_unused]] uint8_t *blob_rep) {}

  virtual auto LookUpBlob([[maybe_unused]] std::span<uint8_t> blob_payload,
                          [[maybe_unused]] const Adapter<RecordBase>::AccessRecordFunc &callback) -> bool {
    return false;
  }
};
