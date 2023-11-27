#pragma once

#include "buffer/buffer_frame.h"
#include "buffer/resident_set.h"
#include "common/typedefs.h"
#include "storage/aio.h"
#include "storage/extent/large_page.h"
#include "storage/free_page_manager.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include "gtest/gtest_prod.h"

#include <linux/exmap.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <atomic>
#include <span>
#include <vector>

namespace leanstore {
// Forward class declaration
class LeanStore;

namespace transaction {
class Transaction;
}

namespace storage {
class FreePageManager;
}

namespace storage::blob {
class BlobManager;
struct PageAliasGuard;
}  // namespace storage::blob

// Exmap env
void HandleExmapSEGFAULT(int signo, siginfo_t *info, void *extra);
void RegisterSEGFAULTHandler();
}  // namespace leanstore

namespace leanstore::buffer {

class BufferManager {
 public:
  explicit BufferManager(std::atomic<bool> &keep_running, storage::FreePageManager *fp);
  BufferManager(u64 virtual_page_count, u64 physical_page_count, u64 extra_page_count, u64 evict_size,
                std::atomic<bool> &keep_running, storage::FreePageManager *fp);
  ~BufferManager();

  // Misc utilities
  auto GetWalInfo() -> std::pair<int, u64>;
  void AllocMetadataPage();
  void RunPageProviderThreads();

  // Free Pages utilities
  auto GetFreePageManager() -> storage::FreePageManager *;

  // Misc helpers
  auto GetPageState(pageid_t page_id) -> sync::PageState &;
  void ValidatePID(pageid_t pid);
  auto IsValidPtr(void *page) -> bool;
  auto ToPID(void *page) -> pageid_t;
  auto ToPtr(pageid_t page_id) -> storage::Page *;
  auto BufferFrame(pageid_t page_id) -> buffer::BufferFrame &;
  void ChunkOperation(pageid_t start_pid, u64 no_bytes, const std::function<void(u64, std::span<u8>)> &func);

  // Main Page operation
  void EnsureFreePages();
  auto AllocPage() -> storage::Page *;
  void HandlePageFault(pageid_t page_id);
  void ReadPage(pageid_t page_id);
  void Evict();

  // Extent utilities
  auto TryReuseExtent(u64 required_page_cnt, bool is_special_block, pageid_t &out_start_pid) -> bool;
  auto AllocExtent(extidx_t extent_idx, u64 fixed_page_cnt) -> pageid_t;
  void PrepareExtentEviction(pageid_t start_pid);
  void EvictExtent(pageid_t pid, u64 page_cnt);
  void ReadExtents(const storage::LargePageList &large_pages);

  // Extra area utilities
  auto RequestAliasingArea(u64 requested_size) -> pageid_t;
  void ReleaseAliasingArea();

  // Synchronization
  auto FixExclusive(pageid_t page_id) -> storage::Page *;
  auto FixShare(pageid_t page_id) -> storage::Page *;
  void UnfixExclusive(pageid_t page_id);
  void UnfixShare(pageid_t page_id);

 private:
  static constexpr u8 NO_BLOCKS_PER_LOCK = sizeof(u64) * CHAR_BIT;

  friend class leanstore::transaction::Transaction;
  friend class leanstore::storage::blob::BlobManager;
  friend struct leanstore::storage::blob::PageAliasGuard;
  friend class leanstore::LeanStore;
  FRIEND_TEST(TestBlobManager, InsertNewBlob);
  FRIEND_TEST(TestBlobManager, GrowExistingBlob);
  FRIEND_TEST(TestBufferManager, BasicTest);
  FRIEND_TEST(TestBufferManager, BasicTestWithExtent);
  FRIEND_TEST(TestBufferManager, AllocFullCapacity);
  FRIEND_TEST(TestBufferManager, SharedAliasingLock);
  FRIEND_TEST(TestBufferManager, ConcurrentRequestAliasingArea);

  // Various private utilities
  void Construction();
  void ExmapAlloc(pageid_t pid, size_t mem_alloc_sz = 1);
  auto FixShareImpl(pageid_t page_id) -> bool;
  auto ToggleShalasLocks(bool set_op, u64 &block_start, u64 block_end) -> bool;
  void PrepareExtentEnv(pageid_t start_pid, u64 page_cnt);
  void PrepareTailExtent(bool already_lock_1st_extent, pageid_t start_pid, u64 page_cnt);

  /* Exmap environment */
  int exmapfd_;
  std::vector<struct exmap_user_interface *> exmap_interface_;

  /* Buffer pool environment */
  const u64 virtual_size_;          /* Size of the buffer pool manager */
  const u64 physical_size_;         /* Size of OS memory used for LeanStore buffer manager */
  const u64 alias_size_;            /* Size of the worker-local memory used for various utilities */
  const u64 virtual_cnt_;           /* Number of supported pages in virtual mem */
  const u64 physical_cnt_;          /* Number of supported physical pages */
  const u64 alias_pg_cnt_;          /* Number of pages in the aliasing region per worker */
  const u64 evict_batch_;           /* Size of batch for eviction */
  std::atomic<bool> *keep_running_; /* Whether the database is running or was stopped */

  /* Buffer pool statistics */
  std::atomic<u64> alloc_cnt_;            /* Number of allocated pages in VMCache */
  std::atomic<u64> physical_used_cnt_;    /* Number of active pages in OS memory */
  std::atomic<u64> maximum_page_size_{1}; /* The maximum size of a large page */

  /* Free Extents/Pages manager */
  storage::FreePageManager *free_pages_;

  /* I/O interfaces */
  int blockfd_;
  std::vector<storage::LibaioInterface> aio_interface_;

  /* Page info & addresses */
  storage::Page *virtual_mem_;   /* The virtual memory addresses to refer to DB pages */
  sync::PageState *page_state_;  /* The state of all database pages */
  buffer::BufferFrame *frame_;   /* The buffer frames of all stored pages */
  ResidentPageSet resident_set_; /* The clock replacer impl */

  /* Aliasing memory regions with block-granular range lock on Shalas area */
  std::vector<u64> wl_alias_ptr_; /* Current Ptr in Worker-local aliasing area */
  std::atomic<u64> shalas_ptr_;   /* Current block ptr in shared aliasing area */
  storage::Page *shalas_area_;    /* Shared-area for memory aliasing */
  const u64 shalas_no_blocks_;    /* Number of blocks in shared aliasing area, every block is as big as alias_size_ */
  const u64 shalas_no_locks_;     /* Number of locks of `shalas_lk_`, each atomic<u64> manages 64 locks */
  std::unique_ptr<std::atomic<u64>[]> shalas_lk_; /* Range lock impl with Bitmap, each bit corresponds to a block */
  std::vector<std::vector<std::pair<u64, u64>>> shalas_lk_acquired_; /* For UNDO the lock after a successful lock */
};

}  // namespace leanstore::buffer