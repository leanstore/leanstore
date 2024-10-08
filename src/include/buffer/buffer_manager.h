#pragma once

#include "buffer/alias_area.h"
#include "buffer/buffer_frame.h"
#include "buffer/resident_set.h"
#include "common/typedefs.h"
#include "storage/aio.h"
#include "storage/extent/large_page.h"
#include "storage/free_storage.h"
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
class LeanStore;

namespace transaction {
class Transaction;
}

namespace storage::blob {
class BlobManager;
struct AliasingGuard;
}  // namespace storage::blob

// Exmap env
void HandleExmapSEGFAULT(int signo, siginfo_t *info, void *extra);
void RegisterSEGFAULTHandler();
}  // namespace leanstore

namespace leanstore::buffer {

class BufferManager {
 public:
  explicit BufferManager(std::atomic<bool> &keep_running);
  BufferManager(u64 virtual_page_count, u64 physical_page_count, u64 extra_page_count, u64 evict_size,
                std::atomic<bool> &keep_running);
  ~BufferManager();

  // Misc APIs
  void AllocMetadataPage();
  void RunPageProviderThreads();
  auto AliasArea() -> AliasingArea *;
  auto FreeStorageManager() -> storage::FreeStorageManager *;

  // Misc helpers
  auto GetPageState(pageid_t page_id) -> sync::PageState &;
  void ValidatePID(pageid_t pid);
  auto IsValidPtr(void *page) -> bool;
  auto ToPID(void *page) -> pageid_t;
  auto ToPtr(pageid_t page_id) -> storage::Page *;
  auto IsExtent(pageid_t page_id) -> bool;
  auto PageIsDirty(pageid_t page_id) -> bool;
  auto BufferFrame(pageid_t page_id) -> buffer::BufferFrame &;
  void ChunkOperation(pageid_t start_pid, u64 no_bytes, const std::function<void(u64, std::span<u8>)> &func);

  // Main page life-cycle
  void EnsureFreePages();
  auto AllocPage() -> storage::Page *;
  void HandlePageFault(pageid_t page_id);
  void ReadPage(pageid_t page_id);
  void Evict();

  // Extent utilities
  auto TryReuseExtent(u64 page_cnt, pageid_t &out_start_pid) -> bool;
  auto AllocExtent(extidx_t tier) -> pageid_t;
  void PrepareExtentEviction(pageid_t start_pid);
  void EvictExtent(pageid_t pid);
  void ReadExtents(const storage::LargePageList &large_pages);
  auto ExtentPageCount(pageid_t pid) -> u64;

  // Synchronization
  auto FixExclusive(pageid_t page_id) -> storage::Page *;
  auto FixShare(pageid_t page_id) -> storage::Page *;
  void UnfixExclusive(pageid_t page_id);
  void UnfixShare(pageid_t page_id);

 private:
  static constexpr u8 NO_BLOCKS_PER_LOCK = sizeof(u64) * CHAR_BIT;

  friend class leanstore::transaction::Transaction;
  friend class leanstore::storage::blob::BlobManager;
  friend struct leanstore::storage::blob::AliasingGuard;
  friend class leanstore::LeanStore;
  template <typename U>
  FRIEND_TEST(TestBlobManager, InsertNewBlob);
  template <typename U>
  FRIEND_TEST(TestBlobManager, GrowExistingBlob);
  FRIEND_TEST(TestBufferManager, BasicTest);
  FRIEND_TEST(TestBufferManager, BasicTestWithExtent);
  FRIEND_TEST(TestBufferManager, AllocFullCapacity);
  FRIEND_TEST(TestBufferManager, SharedAliasingLock);
  FRIEND_TEST(TestBufferManager, ConcurrentRequestAliasingArea);

  /* Various private utilities */
  void Construction();
  void ExmapAlloc(pageid_t pid, size_t mem_alloc_sz = 1);
  auto FixShareImpl(pageid_t page_id) -> bool;

  /* Exmap environment */
  int exmapfd_;
  std::vector<struct exmap_user_interface *> exmap_interface_;

  /* Buffer pool environment */
  const u64 virtual_size_;          /* Maximum size of the database */
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

  /* I/O interfaces */
  int blockfd_;
  std::vector<storage::LibaioInterface> aio_interface_;

  /* Page info & addresses */
  storage::Page *mem_;           /* The virtual memory addresses to refer to DB pages */
  sync::PageState *page_state_;  /* The state of all database pages */
  buffer::BufferFrame *frame_;   /* The buffer frames of all stored pages */
  ResidentPageSet resident_set_; /* The clock replacer impl */

  /* Free storage manager */
  std::unique_ptr<storage::FreeStorageManager> free_storage_;

  /* Aliasing memory regions with range lock on the shared aliasing area */
  std::unique_ptr<AliasingArea> area_;

  /* Extent management */
  const u64 base_pid_;      /* The base pid of the extent buffer manager */
  bool enable_extent_tier_; /* Enable tiering for extent allocation */
  u64 extent_virtual_size_; /* Total size of the extent buffer manager, similar to virtual_size_ */
  u64 extent_virtual_cnt_;  /* Total page cnt of the extent buffer manager, similar to virtual_cnt_ */
  std::array<std::atomic<u64>, leanstore::storage::ExtentList::NO_TIERS>
    tier_alloc_; /* Number of allocated pages in each tier, only used if enable_extent_tier_==true */
};

}  // namespace leanstore::buffer