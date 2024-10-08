#define FUSE_USE_VERSION 35

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/fuse/schema.h"
#include "leanstore/leanstore.h"

#include <fuse.h>
#include <cstring>

struct LeanStoreFUSE {
  static LeanStoreFUSE *obj;
  leanstore::LeanStore *db;
  std::unique_ptr<LeanStoreAdapter<leanstore::fuse::FileRelation>> adapter;

  explicit LeanStoreFUSE(leanstore::LeanStore *db)
      : db(db), adapter(std::make_unique<LeanStoreAdapter<leanstore::fuse::FileRelation>>(*db)) {}

  ~LeanStoreFUSE() = default;

  static auto GetAttr(const char *path, struct stat *stbuf) -> int {
    std::string filename = path;
    int res              = 0;
    memset(stbuf, 0, sizeof(struct stat));

    stbuf->st_uid   = getuid();
    stbuf->st_gid   = getgid();
    stbuf->st_atime = stbuf->st_mtime = stbuf->st_ctime = time(nullptr);

    if (filename == "/") {
      stbuf->st_mode  = S_IFDIR | 0777;
      stbuf->st_nlink = 2;
    } else {
      obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
        obj->db->StartTransaction();
        uint8_t blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
        uint64_t blob_rep_size = 0;

        auto file_path = FilePath(path);
        auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);

        auto found = obj->adapter->LookUp(file_key, [&](const auto &rec) {
          blob_rep_size = rec.PayloadSize();
          std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
        });
        if (!found) {
          res = -ENOENT;
          obj->db->CommitTransaction();
          return;
        }

        stbuf->st_mode  = S_IFREG | 0777;
        stbuf->st_nlink = 1;
        stbuf->st_size  = reinterpret_cast<leanstore::BlobState *>(blob_rep)->blob_size;
        obj->db->CommitTransaction();
      });
    }

    return res;
  }

  static auto Open([[maybe_unused]] const char *path, [[maybe_unused]] struct fuse_file_info *inf) -> int { return 0; }

  static auto Read(const char *path, char *buf, size_t size, off_t offset,
                   [[maybe_unused]] struct fuse_file_info *inf) -> int {
    int ret = 0;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();
      uint8_t blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
      uint64_t blob_rep_size = 0;

      auto file_path = FilePath(path);
      auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);

      auto found = obj->adapter->LookUp(file_key, [&](const auto &rec) {
        blob_rep_size = rec.PayloadSize();
        std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
      });
      if (!found) {
        ret = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }

      auto bh = reinterpret_cast<leanstore::BlobState *>(blob_rep);
      if (static_cast<u64>(offset) >= bh->blob_size) {
        ret = -EFAULT;
        obj->db->CommitTransaction();
        return;
      }

      obj->db->LoadBlob(
        bh, [&](std::span<const u8> content) { std::memcpy(buf, content.data() + offset, size); }, false);

      ret = std::min(size, bh->blob_size - offset);
      obj->db->CommitTransaction();
    });

    return ret;
  }
};

LeanStoreFUSE *LeanStoreFUSE::obj;

/**
 * @brief FUSE interface
 *
 * Command: `./test/LeanStoreFUSE -d -s -f /mnt/test`
 *
 * Read program:
 *
 * #include <fcntl.h>
 * #include <sys/stat.h>
 * #include <sys/syscall.h>
 * #include <cstdio>
 * #include <cstdlib>
 * #include <unistd.h>
 *
 * int main(){
 *     //./yfs1 is the mount point of my filesystem
 *     int fd = open("/mnt/test/hello", O_RDONLY);
 *     auto buf = (char *)malloc(4096);
 *     int readsize = read(fd, buf, 4096);
 *     printf("%d,%s,%d\n",fd, buf, readsize);
 *     close(fd);
 * }
 */
auto main(int argc, char **argv) -> int {
  // Initialize FUSE filesystem
  FLAGS_worker_count   = 1;
  FLAGS_bm_virtual_gb  = 128;
  FLAGS_bm_physical_gb = 32;
  FLAGS_db_path        = "/dev/nvme1n1";
  auto db              = std::make_unique<leanstore::LeanStore>();
  auto fs              = LeanStoreFUSE(db.get());
  LeanStoreFUSE::obj   = &fs;

  // Initialize temp BLOB
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    u8 payload[4096];
    for (auto &c : payload) { c = 123; }
    auto blob_rep = db->CreateNewBlob({payload, 4096}, {}, false);
    fs.adapter->InsertRawPayload({"/hello"}, blob_rep);
    db->CommitTransaction();
  });

  struct fuse_operations fs_oper;
  fs_oper.open    = LeanStoreFUSE::Open;
  fs_oper.read    = LeanStoreFUSE::Read;
  fs_oper.getattr = LeanStoreFUSE::GetAttr;

  return fuse_main(argc, argv, &fs_oper, nullptr);
}