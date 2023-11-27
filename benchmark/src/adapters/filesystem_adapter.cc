#include "benchmark/adapters/filesystem_adapter.h"
#include "benchmark/adapters/config.h"
#include "benchmark/gitclone/schema.h"
#include "benchmark/utils/test_utils.h"
#include "benchmark/wikipedia/schema.h"
#include "benchmark/ycsb/schema.h"

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <filesystem>
#include <iostream>
#include <string>

#define READ_ENTRY(path, record)                                                                       \
  ({                                                                                                   \
    [[maybe_unused]] auto size = ReadPayload((path), payload);                                         \
    (record)                   = reinterpret_cast<RecordBase *>(payload.get());                        \
    if constexpr (requires { (record)->payload; }) { assert((record)->payload.MallocSize() == size); } \
  })

#define FILE_SIZE(fd)                                  \
  ({                                                   \
    struct stat file_stat;                             \
    [[maybe_unused]] auto ret = fstat(fd, &file_stat); \
    assert(ret == 0);                                  \
    file_stat.st_size;                                 \
  })

// -------------------------------------------------------------------------------------
FilesystemAsDB::FilesystemAsDB(const std::string &root_path, bool enable_fsync, bool track_storage_size)
    : root_dir(fs::path(root_path)), enable_fsync(enable_fsync), track_storage_size(track_storage_size) {
  fs::remove_all(root_dir);
  auto cmd                  = std::string("mkdir -p ") + root_path;
  [[maybe_unused]] auto ret = system(cmd.c_str());
}

void FilesystemAsDB::DropCache() {
  LOG_INFO("Dropping the cache");
  [[maybe_unused]] auto ret = system("sudo sh -c \"sync; /usr/bin/echo 3 > /proc/sys/vm/drop_caches\"");
  assert(ret >= 0);
  LOG_INFO("Complete dropping page cache");
}

auto FilesystemAsDB::StartProfilingThread(std::atomic<bool> &keep_running, std::atomic<uint64_t> &completed_txn)
  -> std::thread {
  return std::thread([&]() {
    u64 cnt = 0;
    if (track_storage_size) {
      std::printf("system,ts,tx,size_gb,f_blocks,f_bavail\n");
    } else {
      std::printf("system,ts,tx\n");
    }

    while (keep_running.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      auto progress = completed_txn.exchange(0);
      total_txn_completed += progress;
      if (track_storage_size) {
        struct statvfs buf;
        statvfs(root_dir.c_str(), &buf);
        auto db_size = static_cast<float>((buf.f_blocks - buf.f_bavail) * buf.f_frsize) / (1024 * 1024 * 1024);
        std::printf("filesystem,%lu,%lu,%.4f,%lu,%lu\n", cnt++, progress, db_size, buf.f_blocks, buf.f_bavail);
      } else {
        std::printf("filesystem,%lu,%lu\n", cnt++, progress);
      }
    }
    std::printf("Halt Profiling thread\n");
  });
}

auto FilesystemAsDB::DatabaseSize() -> float {
  auto ret = 0UL;
  for (const auto &entry : fs::directory_iterator(root_dir)) { ret += fs::file_size(entry); }
  return static_cast<float>(ret) / (1024 * 1024 * 1024);
}

// -------------------------------------------------------------------------------------
template <class RecordBase>
thread_local std::unique_ptr<EVP_MD_CTX, FilesystemAsDB::EvlDeleter> FilesystemAdapter<RecordBase>::sha_context =
  std::unique_ptr<EVP_MD_CTX, FilesystemAsDB::EvlDeleter>{EVP_MD_CTX_create()};

template <class RecordBase>
FilesystemAdapter<RecordBase>::FilesystemAdapter(FilesystemAsDB *db) : db_(db) {}

template <class RecordBase>
auto FilesystemAdapter<RecordBase>::ReadPayload(const fs::path &entry,
                                                std::unique_ptr<uint8_t[], FreeDelete> &out_payload) -> size_t {
  auto fd = open(entry.c_str(), O_RDONLY, S_IRWXU);
  assert(fd >= 0);
  auto payload_size = FILE_SIZE(fd);
  out_payload.reset(reinterpret_cast<uint8_t *>(malloc(payload_size)));
  [[maybe_unused]] auto ret = pread(fd, out_payload.get(), payload_size, 0);
  assert(ret == static_cast<int>(payload_size));
  close(fd);
  return payload_size;
}

template <class RecordBase>
void FilesystemAdapter<RecordBase>::FileStat(const typename RecordBase::Key &r_key) {
  auto path = db_->root_dir / r_key.String();
  if (fs::exists(path)) {
    struct stat file_stat;
    auto fd = open(path.c_str(), O_RDONLY, S_IRWXU);
    assert(fd >= 0);
    [[maybe_unused]] auto ret = fstat(fd, &file_stat);
    assert(ret == 0);
    close(fd);
  }
}

/**
 * @brief Extra op to create a fairer comparison between Filesystem and LeanStore
 * This includes:
 * - sha256 calculation
 */
template <class RecordBase>
void FilesystemAdapter<RecordBase>::ExtraOperator(std::span<const uint8_t> payload) {
  uint8_t sha2_val[sha256::SHA256_DIGEST_SIZE];
  EVP_DigestInit_ex(sha_context.get(), EVP_sha256(), nullptr);
  EVP_DigestUpdate(sha_context.get(), payload.data(), payload.size());
  EVP_DigestFinal_ex(sha_context.get(), sha2_val, nullptr);
}

// -------------------------------------------------------------------------------------
template <class RecordBase>
void FilesystemAdapter<RecordBase>::Scan(const typename RecordBase::Key &key,
                                         const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  auto key_str = key.String();

  // -------------------------------------------------------------------------------------
  for (const auto &entry : fs::directory_iterator(db_->root_dir)) {
    auto filename = std::string(fs::path(entry).filename());
    if (filename.compare(key_str) <= 0) {
      std::unique_ptr<uint8_t[], FreeDelete> payload = nullptr;
      RecordBase *record                             = nullptr;
      READ_ENTRY(entry, record);

      // Deserialize key & payload and Read
      typename RecordBase::Key s_key;
      s_key.FromString(filename);
      if (!found_record_cb(s_key, *record)) { break; }
    }
  }
}

template <class RecordBase>
void FilesystemAdapter<RecordBase>::ScanDesc(const typename RecordBase::Key &key,
                                             const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  auto key_str = key.String();

  // -------------------------------------------------------------------------------------
  for (const auto &entry : fs::directory_iterator(db_->root_dir)) {
    auto filename = std::string(fs::path(entry).filename());
    if (filename.compare(key_str) >= 0) {
      std::unique_ptr<uint8_t[], FreeDelete> payload = nullptr;
      RecordBase *record                             = nullptr;
      READ_ENTRY(entry, record);

      // Deserialize key & payload and Read
      typename RecordBase::Key s_key;
      s_key.FromString(filename);
      if (!found_record_cb(s_key, *record)) { break; }
    }
  }
}

template <class RecordBase>
void FilesystemAdapter<RecordBase>::Insert(const typename RecordBase::Key &r_key, const RecordBase &record) {
  auto path = db_->root_dir / r_key.String();
  auto fd   = open(path.c_str(), O_RDWR | O_CREAT, S_IRWXU);
  assert(fd >= 0);
  [[maybe_unused]] int ret = pwrite(fd, &record, record.PayloadSize(), 0);
  assert(ret == static_cast<int>(record.PayloadSize()));
  ExtraOperator({reinterpret_cast<const uint8_t *>(&record), record.PayloadSize()});
  if (db_->enable_fsync) {
    if (rand() % FLAGS_fs_fsync_rate == 0) { sync(); }
  }
  close(fd);
}

template <class RecordBase>
void FilesystemAdapter<RecordBase>::InsertRawPayload(const typename RecordBase::Key &r_key,
                                                     std::span<const uint8_t> record) {
  auto path = db_->root_dir / r_key.String();
  auto fd   = open(path.c_str(), O_RDWR | O_CREAT, S_IRWXU);
  assert(fd >= 0);
  [[maybe_unused]] int ret = pwrite(fd, record.data(), record.size(), 0);
  Ensure(ret == static_cast<int>(record.size()));
  ExtraOperator(record);
  if (db_->enable_fsync) {
    if (rand() % FLAGS_fs_fsync_rate == 0) { sync(); }
  }
  close(fd);
}

template <class RecordBase>
void FilesystemAdapter<RecordBase>::Update(const typename RecordBase::Key &r_key, const RecordBase &record) {
  auto path = db_->root_dir / r_key.String();
  auto fd   = open(path.c_str(), O_RDWR | O_CREAT, S_IRWXU);
  assert(fd >= 0);
  {
    /**
     * Resize the file to correct setting if necessary
     */
    struct stat fs;
    [[maybe_unused]] auto ret = fstat(fd, &fs);
    assert(ret >= 0);
    if (fs.st_size != record.PayloadSize()) {
      ret = ftruncate(fd, record.PayloadSize());
      assert(ret == 0);
    }
  }
  [[maybe_unused]] int ret = pwrite(fd, &record, record.PayloadSize(), 0);
  assert(ret == static_cast<int>(record.PayloadSize()));
  ExtraOperator({reinterpret_cast<const uint8_t *>(&record), record.PayloadSize()});
  if (db_->enable_fsync) {
    if (rand() % FLAGS_fs_fsync_rate == 0) { sync(); }
  }
  close(fd);
}

template <class RecordBase>
auto FilesystemAdapter<RecordBase>::LookUp(const typename RecordBase::Key &r_key,
                                           const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool {
  auto path = db_->root_dir / r_key.String();
  if (!fs::exists(path)) { return false; }
  std::unique_ptr<uint8_t[], FreeDelete> payload = nullptr;
  RecordBase *record                             = nullptr;
  READ_ENTRY(path, record);
  fn(*record);
  return true;
}

template <class RecordBase>
void FilesystemAdapter<RecordBase>::UpdateInPlace(const typename RecordBase::Key &r_key,
                                                  const typename Adapter<RecordBase>::ModifyRecordFunc &fn) {
  auto path = db_->root_dir / r_key.String();
  if (fs::exists(path)) {
    auto fd = open(path.c_str(), O_RDWR, S_IRWXU);
    assert(fd >= 0);
    auto payload_size = FILE_SIZE(fd);

    // Read and deserialize the payload
    std::unique_ptr<uint8_t[], FreeDelete> payload(reinterpret_cast<uint8_t *>(malloc(payload_size)));
    [[maybe_unused]] int ret = pread(fd, payload.get(), payload_size, 0);
    assert(ret == static_cast<int>(payload_size));
    auto &record = *reinterpret_cast<RecordBase *>(payload.get());
    assert(record.PayloadSize() == payload_size);

    // Modify the record and write it
    fn(record);
    ret = ftruncate(fd, record.PayloadSize());
    assert(ret == 0);
    ret = pwrite(fd, payload.get(), record.PayloadSize(), 0);
    ExtraOperator({payload.get(), record.PayloadSize()});
    assert(ret == static_cast<int>(record.PayloadSize()));
    if (db_->enable_fsync) {
      if (rand() % FLAGS_fs_fsync_rate == 0) { sync(); }
    }
    close(fd);
  }
}

template <class RecordBase>
auto FilesystemAdapter<RecordBase>::Erase(const typename RecordBase::Key &r_key) -> bool {
  auto path = db_->root_dir / r_key.String();
  return fs::remove(path);
}

template <class RecordBase>
auto FilesystemAdapter<RecordBase>::Count() -> uint64_t {
  return std::distance(fs::directory_iterator(db_->root_dir), fs::directory_iterator{});
}

// For testing purpose
template class FilesystemAdapter<benchmark::RelationTest>;
template class FilesystemAdapter<benchmark::VariableSizeRelation>;

// For YCSB
template class FilesystemAdapter<benchmark::FileRelation<0, 120>>;
template class FilesystemAdapter<benchmark::FileRelation<0, 4096>>;
template class FilesystemAdapter<benchmark::FileRelation<0, 102400>>;
template class FilesystemAdapter<benchmark::FileRelation<0, 1048576>>;
template class FilesystemAdapter<benchmark::FileRelation<0, 10485760>>;

// For Wikipedia Read-Only workload
template class FilesystemAdapter<wiki::FileRelation>;

// For Git Clone workload
template class FilesystemAdapter<gitclone::SystemFileRelation>;
template class FilesystemAdapter<gitclone::TemplateFileRelation>;
template class FilesystemAdapter<gitclone::ObjectFileRelation>;
