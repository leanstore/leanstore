#pragma once

#include "benchmark/adapters/adapter.h"
#include "benchmark/adapters/config.h"
#include "benchmark/utils/misc.h"

#include "openssl/evp.h"
#include "share_headers/config.h"

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>

namespace fs = std::filesystem;

/**
 * @brief All experimented filesystems:
 * - ext4 data=ordered with fsync enabled
 * - ext4 data=journal with fsync disabled
 * - f2fs no compression with fsync disabled
 * - btrfs with fsync disabled
 */
struct FilesystemAsDB {
  fs::path root_dir;
  bool enable_fsync;
  bool track_storage_size;
  std::atomic<uint64_t> total_txn_completed = 0;

  struct EvlDeleter {
    void operator()(EVP_MD_CTX *ptr) const { EVP_MD_CTX_destroy(ptr); }
  };

  FilesystemAsDB(const std::string &root_path, bool enable_fsync, bool track_storage_size = false);
  ~FilesystemAsDB() = default;

  void DropCache();
  auto DatabaseSize() -> float;
  auto StartProfilingThread(std::atomic<bool> &keep_running, std::atomic<uint64_t> &completed_txn) -> std::thread;
};

template <class RecordBase>
class FilesystemAdapter : public Adapter<RecordBase> {
 private:
  FilesystemAsDB *db_;

 public:
  static thread_local std::unique_ptr<EVP_MD_CTX, FilesystemAsDB::EvlDeleter> sha_context;

  explicit FilesystemAdapter(FilesystemAsDB *db);
  ~FilesystemAdapter() override = default;

  // -------------------------------------------------------------------------------------
  auto ReadPayload(const fs::path &entry, std::unique_ptr<uint8_t[], FreeDelete> &out_payload) -> size_t;
  void FileStat(const typename RecordBase::Key &r_key);
  void ExtraOperator(std::span<const uint8_t> payload);

  // -------------------------------------------------------------------------------------
  void Scan(const typename RecordBase::Key &key,
            const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) override;
  void ScanDesc(const typename RecordBase::Key &key,
                const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) override;
  void Insert(const typename RecordBase::Key &r_key, const RecordBase &record) override;
  void InsertRawPayload(const typename RecordBase::Key &r_key, std::span<const uint8_t> record);
  void Update(const typename RecordBase::Key &r_key, const RecordBase &record) override;
  auto LookUp(const typename RecordBase::Key &r_key, const typename Adapter<RecordBase>::AccessRecordFunc &fn)
    -> bool override;
  void UpdateInPlace(const typename RecordBase::Key &r_key,
                     const typename Adapter<RecordBase>::ModifyRecordFunc &fn) override;
  auto Erase(const typename RecordBase::Key &r_key) -> bool override;

  // -------------------------------------------------------------------------------------
  auto Count() -> uint64_t override;
};
