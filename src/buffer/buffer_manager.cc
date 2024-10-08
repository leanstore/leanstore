#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/format.h"
#include "common/rand.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "storage/aio.h"
#include "storage/blob/blob_state.h"

#include "cpptrace/cpptrace.hpp"
#include "fmt/ranges.h"
#include "share_headers/logger.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <utility>
#include <vector>

/**
 * @brief There is no need to call GetPageState(pid).Init() during page/extent allocation
 *    because the page state will be automatically X-lock acquired
 */
#define NEW_ALLOC_PAGE(pid)                     \
  ({                                            \
    auto &ps     = GetPageState(pid);           \
    u64 v        = ps.StateAndVersion().load(); \
    bool success = ps.TryLockExclusive(v);      \
    Ensure(success);                            \
    frame_[pid].Init();                         \
  })

#define PAGE_FAULT(pid, page_cnt)     \
  ({                                  \
    physical_used_cnt_ += (page_cnt); \
    EnsureFreePages();                \
    ExmapAlloc(pid, page_cnt);        \
  })

#define PREPARE_EXTENT(pid, page_cnt)                                               \
  ({                                                                                \
    resident_set_.Insert(pid);                                                      \
    if (!enable_extent_tier_) { frame_[pid].extent_size = (page_cnt); }             \
    if (FLAGS_bm_enable_fair_eviction) { UpdateMax(maximum_page_size_, page_cnt); } \
  })

using leanstore::storage::ExtentList;

namespace leanstore {

void HandleExmapSEGFAULT([[maybe_unused]] int signo, siginfo_t *info, [[maybe_unused]] void *extra) {
  void *page = info->si_addr;
  for (auto &buffer_pool : all_buffer_pools) {
    if (buffer_pool->IsValidPtr(page)) {
      LOG_DEBUG("SEGFAULT restart - Page %lu", buffer_pool->ToPID(page));
      throw sync::RestartException();
    }
  }
  LOG_ERROR("SEGFAULT - addr %p", page);
  cpptrace::generate_trace().print();
  throw ex::EnsureFailed("SEGFAULT");
}

void RegisterSEGFAULTHandler() {
  struct sigaction action;
  action.sa_flags     = SA_SIGINFO;
  action.sa_sigaction = HandleExmapSEGFAULT;
  if (sigaction(SIGSEGV, &action, nullptr) == -1) {
    perror("sigusr: sigaction");
    throw leanstore::ex::EnsureFailed();
  }
}

}  // namespace leanstore

namespace leanstore::buffer {

BufferManager::BufferManager(std::atomic<bool> &keep_running)
    : virtual_size_(FLAGS_bm_virtual_gb * GB),
      physical_size_(FLAGS_bm_physical_gb * GB),
      alias_size_(FLAGS_bm_alias_block_mb * MB),
      virtual_cnt_(virtual_size_ / PAGE_SIZE),
      physical_cnt_(physical_size_ / PAGE_SIZE),
      alias_pg_cnt_(alias_size_ / PAGE_SIZE),
      evict_batch_(FLAGS_bm_evict_batch_size),
      keep_running_(&keep_running),
      resident_set_(physical_cnt_),
      free_storage_(std::make_unique<storage::FreeStorageManager>()),
      base_pid_(virtual_cnt_ + 1),
      enable_extent_tier_(FLAGS_blob_buffer_pool_gb == 0) {
  Construction();
}

BufferManager::BufferManager(u64 virtual_page_count, u64 physical_page_count, u64 extra_page_count, u64 evict_size,
                             std::atomic<bool> &keep_running)
    : virtual_size_(virtual_page_count * PAGE_SIZE),
      physical_size_(physical_page_count * PAGE_SIZE),
      alias_size_(extra_page_count * PAGE_SIZE),
      virtual_cnt_(virtual_page_count),
      physical_cnt_(physical_page_count),
      alias_pg_cnt_(extra_page_count),
      evict_batch_(evict_size),
      keep_running_(&keep_running),
      resident_set_(physical_cnt_),
      free_storage_(std::make_unique<storage::FreeStorageManager>()),
      base_pid_(virtual_cnt_ + 1),
      enable_extent_tier_(FLAGS_blob_buffer_pool_gb == 0) {
  Construction();
}

BufferManager::~BufferManager() {
  close(exmapfd_);
  close(blockfd_);
}

/**
 * @brief Format of the virtual memory buffer pool is as followed:
 *
 * The virtual memory area comprises of three main components
 *
 * - Main buffer manager (virtual_size_ + PAGE_SIZE):
 *  + bm:   the buffer pool manager (vmcache)
 *  + exp:  extra page, to prevent segfaults during optimistic reads
 *
 * - Extent buffer manager for extent accesses/alloc
 *  + extent-bm: tiering vs random+huge:
 *    * tiering: M vmcache for M tiers, each vm is as big as the main buffer pool,
 *          i.e. # elements is `virtual_size_ / TIER_SIZE[index]`
 *            |------|------|----|------|
 *            | t-ht | t-ht |....| t-ht |
 *            |------|------|----|------|
 *    * random+huge: 1 huge virtual memory range, defined by FLAGS_blob_buffer_pool_gb
 *          During allocation, we randomly pick a free range and assign that PID to the extent
 *            |--------------------------------|
 *            | FLAGS_blob_buffer_pool_gb * GB |
 *            |--------------------------------|
 *
 * - Aliasing area (alias_size_ * FLAGS_worker_count + virtual_size_):
 *  + wl:     N worker-local aliasing area for N workers
 *  + shalar: shared-area for aliasing, same size with the main buffer pool
 *
 * The virtual memory area, therefore, looks like the following figure:
 *  |--------------------|-----|-----------|----|----|----|----|--------|
 *  | bm (virtual_size_) | exp | extent-bm | wl | wl |....| wl | shalar |
 *  |--------------------|-----|-----------|----|----|----|----|--------|
 *
 * Note that ptrs belong to `wl` and `shalar` are considered INVALID by all components except BlobManager
 * All of the addresses should be PAGE_SIZE aligned
 */
void BufferManager::Construction() {
  assert(virtual_size_ >= physical_size_);
  blockfd_ = open(FLAGS_db_path.c_str(), O_RDWR | O_DIRECT, S_IRWXU);
  Ensure(blockfd_ > 0);

  // Setup virtual memory for vmcache
  if (enable_extent_tier_) {
    extent_virtual_size_ = ExtentList::NO_TIERS * virtual_size_;
    extent_virtual_cnt_  = ExtentList::NO_TIERS * virtual_cnt_;
  } else {
    extent_virtual_size_ = FLAGS_blob_buffer_pool_gb * GB;
    extent_virtual_cnt_  = extent_virtual_size_ / PAGE_SIZE;
  }
  auto virtual_alloc_size =
    virtual_size_ + PAGE_SIZE + extent_virtual_size_ + alias_size_ * FLAGS_worker_count + virtual_size_;

  // exmap allocation
  exmapfd_ = open(FLAGS_exmap_path.c_str(), O_RDWR);
  if (exmapfd_ < 0) {
    throw leanstore::ex::GenericException("Open exmap file-descriptor error. Did you load the module?");
  }

  // Workers + Page providers + 1 Group Commit thread
  auto no_interfaces = FLAGS_worker_count + FLAGS_page_provider_thread + 1;

  struct exmap_ioctl_setup buffer;
  buffer.fd             = blockfd_;
  buffer.max_interfaces = no_interfaces;
  buffer.buffer_size    = physical_cnt_;
  auto ret              = ioctl(exmapfd_, EXMAP_IOCTL_SETUP, &buffer);
  if (ret < 0) { throw leanstore::ex::GenericException("ioctl: exmap_setup error"); }

  exmap_interface_.resize(no_interfaces);
  for (size_t idx = 0; idx < no_interfaces; idx++) {
    exmap_interface_[idx] = static_cast<exmap_user_interface *>(
      mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, exmapfd_, EXMAP_OFF_INTERFACE(idx)));
    if (exmap_interface_[idx] == MAP_FAILED) { throw leanstore::ex::GenericException("Setup exmap_interface_ error"); };
  }

  // Setup virtual mem on top of exmap
  mem_ =
    static_cast<storage::Page *>(mmap(nullptr, virtual_alloc_size, PROT_READ | PROT_WRITE, MAP_SHARED, exmapfd_, 0));
  if (mem_ == MAP_FAILED) { throw leanstore::ex::GenericException("mmap failed"); };

  // Construct AIO interfaces for writing pages
  aio_interface_.reserve(no_interfaces);
  for (size_t idx = 0; idx < no_interfaces; idx++) { aio_interface_.emplace_back(blockfd_, mem_); }

  // Setup page run-time
  auto total_pg = virtual_cnt_ + extent_virtual_cnt_;
  page_state_   = static_cast<sync::PageState *>(AllocHuge((total_pg) * sizeof(sync::PageState)));
  frame_        = static_cast<buffer::BufferFrame *>(AllocHuge((total_pg) * sizeof(buffer::BufferFrame)));
  area_         = std::make_unique<AliasingArea>(alias_size_, virtual_size_, &mem_[total_pg + 1]);
  Ensure(page_state_ != MAP_FAILED && frame_ != MAP_FAILED);

  // Statistics and summary
  physical_used_cnt_ = 0;
  alloc_cnt_         = 0;
  LOG_INFO(
    "VMCache: Path(%s), AllocSizeGB(%lu), VirtualGB(%lu), VirtualCount(%lu), PhysGB(%lu), PhysCount(%lu), "
    "EvictSize(%lu)",
    FLAGS_db_path.c_str(), virtual_alloc_size / GB, virtual_size_ / GB, virtual_cnt_, physical_size_ / GB,
    physical_cnt_, evict_batch_);
}

// ------------------------------------------------------------------------------------------------------

// Alloc page 0 for metadata
void BufferManager::AllocMetadataPage() {
  AllocPage();
  GetPageState(0).UnlockExclusive();
}

void BufferManager::RunPageProviderThreads() {
  for (u32 t_id = 0; t_id < FLAGS_page_provider_thread; t_id++) {
    std::thread page_provider([&, t_id]() {
      pthread_setname_np(pthread_self(), "page_provider");
      worker_thread_id = FLAGS_worker_count + 1 + t_id;
      try {
        while (keep_running_->load()) {
          if (physical_used_cnt_ >= physical_cnt_ * 0.9) { Evict(); }
          AsmYield();
        }
      } catch (...) {
        // Similar reason to GroupCommitExecutor::StartExecution()
      }
    });
    page_provider.detach();
  }
}

auto BufferManager::AliasArea() -> AliasingArea * { return area_.get(); }

auto BufferManager::FreeStorageManager() -> storage::FreeStorageManager * { return free_storage_.get(); }

// ------------------------------------------------------------------------------------------------------

auto BufferManager::GetPageState(pageid_t page_id) -> sync::PageState & { return page_state_[page_id]; }

void BufferManager::ValidatePID(pageid_t pid) {
  if (pid >= virtual_cnt_ + extent_virtual_cnt_) {
    throw std::runtime_error("Page id " + std::to_string(pid) + " is invalid");
  }
}

auto BufferManager::IsValidPtr(void *page) -> bool {
  return (page >= mem_) && (page < (mem_ + virtual_size_ + PAGE_SIZE + extent_virtual_size_));
}

auto BufferManager::ToPID(void *page) -> pageid_t { return reinterpret_cast<storage::Page *>(page) - mem_; }

auto BufferManager::ToPtr(pageid_t page_id) -> storage::Page * {
  if (FLAGS_blob_normal_buffer_pool) { resident_set_.Contain(page_id); }
  return &mem_[page_id];
}

auto BufferManager::IsExtent(pageid_t page_id) -> bool { return page_id >= virtual_cnt_ + 1; }

auto BufferManager::PageIsDirty(pageid_t page_id) -> bool {
  return frame_[page_id].last_written_gsn < ToPtr(page_id)->p_gsn;
}

auto BufferManager::BufferFrame(pageid_t page_id) -> buffer::BufferFrame & { return frame_[page_id]; }

/**
 * @brief Chunk operation to simulate overheads of normal Buffer Manager
 */
void BufferManager::ChunkOperation(pageid_t start_pid, u64 no_bytes,
                                   const std::function<void(u64, std::span<u8>)> &func) {
  Ensure(FLAGS_blob_normal_buffer_pool);
  auto offset = 0UL;
  while (offset < no_bytes) {
    auto to_op_size = std::min(no_bytes - offset, PAGE_SIZE);
    auto to_op_pid  = start_pid + offset / PAGE_SIZE;
    func(offset, {reinterpret_cast<u8 *>(ToPtr(to_op_pid)), to_op_size});
    offset += to_op_size;
  }
  Ensure(offset == no_bytes);
}

// ------------------------------------------------------------------------------------------------------

void BufferManager::EnsureFreePages() {
  if (FLAGS_page_provider_thread > 0) {
    // Page Provider thread is enabled, then we should sleep to wait for free pages
    while ((physical_used_cnt_ >= physical_cnt_ * 0.95) && (keep_running_->load())) { AsmYield(); }
    return;
  }
  while ((physical_used_cnt_ >= physical_cnt_ * 0.9) && (keep_running_->load())) { Evict(); }
}

void BufferManager::ExmapAlloc(pageid_t pid, size_t mem_alloc_sz) {
  // TODO(XXX): Support ExmapAlloc with `mem_alloc_sz > EXMAP_PAGE_MAX_PAGES * EXMAP_USER_INTERFACE_PAGES`
  int idx = 0;
  for (; mem_alloc_sz > 0; idx++) {
    auto alloc_sz = std::min(static_cast<size_t>(EXMAP_PAGE_MAX_PAGES - 1), mem_alloc_sz);
    exmap_interface_[worker_thread_id]->iov[idx].page = pid;
    exmap_interface_[worker_thread_id]->iov[idx].len  = alloc_sz;
    pid += alloc_sz;
    mem_alloc_sz -= alloc_sz;
  }
  Ensure(idx < EXMAP_USER_INTERFACE_PAGES);
  while (ExmapAction(exmapfd_, EXMAP_OP_ALLOC, idx) < 0) {
    LOG_ERROR("Exmap Alloc Page errno '%d', page_id '%lu', worker_id '%d'", errno, pid, worker_thread_id);
    EnsureFreePages();
  }
}

/**
 * @brief Allocate a single page
 * Always allocate the page at the end of database for simplicity
 */
auto BufferManager::AllocPage() -> storage::Page * {
  pageid_t pid = alloc_cnt_.fetch_add(1, std::memory_order_relaxed);
  ValidatePID(pid);
  PAGE_FAULT(pid, 1);
  NEW_ALLOC_PAGE(pid);
  resident_set_.Insert(pid);
  mem_[pid].p_gsn = 1;
  assert(PageIsDirty(pid));
  return ToPtr(pid);
}

/**
 * @brief Try to ensure that there are free frames (i.e. virtual mem in vmcache) in the buffer manager.
 *        After that, read the page and load it into the buffer pool
 */
void BufferManager::HandlePageFault(pageid_t page_id) {
  assert(GetPageState(page_id).LockState() == sync::PageStateMode::EXCLUSIVE);
  physical_used_cnt_++;
  EnsureFreePages();
  ReadPage(page_id);
  resident_set_.Insert(page_id);
}

void BufferManager::ReadPage(pageid_t page_id) {
  struct iovec vec[1];
  vec[0].iov_base = ToPtr(page_id);
  vec[0].iov_len  = PAGE_SIZE;
  int ret         = preadv(exmapfd_, &vec[0], 1, worker_thread_id);
  Ensure(ret == PAGE_SIZE);
  statistics::buffer::read_cnt++;
}

void BufferManager::Evict() {
  std::vector<pageid_t> to_evict(0);  // store all clean MARKED pages
  std::vector<pageid_t> to_write(0);  // store all dirty MARKED pages

  // 0. find evict candidates, lock dirty ones in shared mode
  //  we should only loop only 2 circles around the clock replacer
  //  - 1st loop is to mark all UNLOCKED -> MARKED
  //  - 2nd loop is to evaluate those MARKED candidate
  //  if we loop indefinitely, we can run into deadlock situation when the buffer pool is full of EXCLUSIVE pages
  for (u64 idx = 0; idx < resident_set_.Capacity(); idx++) {
    // we gather enough pages for the eviction batch, break
    if (to_evict.size() + to_write.size() >= evict_batch_) { break; }
    auto flushed_log_gsn = recovery::LogManager::global_min_gsn_flushed.load();
    resident_set_.IterateClockBatch(evict_batch_, [this, &to_evict, &to_write, &flushed_log_gsn](pageid_t pid) {
      auto page     = ToPtr(pid);
      auto &ps      = GetPageState(pid);
      u64 v         = ps.StateAndVersion();
      auto pg_count = ExtentPageCount(pid);

      switch (sync::PageState::LockState(v)) {
        /**
         * Finite state machine:
         * - UNLOCKED: The page is free (i.e. unused), marked for possible eviction
         * - SHARED: The page is being read (i.e. hot), ignore
         * - EXCLUSIVE: The page is being modified (i.e. hot), ignore
         * - MARKED: The page is marked (i.e. candidiate for eviction) by clock replacer
         *          Check whether we should write it before evict
         * - EVICTED: Already evicted, ignore
         */
        case sync::PageState::MARKED:
          // If this page is prevented to evict, don't do it
          if (frame_[pid].prevent_evict.load()) { return; }
          // Whether to evict this page or not according to priority policy
          if (FLAGS_bm_enable_fair_eviction) {
            if (rand() % maximum_page_size_.load() > pg_count) { return; }
          }
          /**
           * @brief Add this page to to_write if
           * - This page is a normal page (not a BLOB page) and dirty
           *
           * Extent is always clean, so there is no need to write it to the storage
           */
          if (!IsExtent(pid) && PageIsDirty(pid)) {
            /**
             * @brief 2 conditions:
             * - prevent normal pages to be written to disk before its lastest log has been flushed
             *    (note that blob pages don't associate with any log record, hence this condition doesn't apply)
             * - prevents the page from entering (EXCLUSIVE, MARKED, EVICTED) state
             */
            if ((IsExtent(pid) || page->p_gsn <= flushed_log_gsn) && (ps.TryLockShared(v))) { to_write.push_back(pid); }
          } else {
            /**
             * @brief In the case of blob page, eviction reaches this stage only when the blob content is flushed
             * That is, if the blob is not flushed yet, its PageState should be EXCLUSIVE,
             *  and all EXCLUSIVE pages can't be moved to any other state before being moved to UNLOCKED
             */
            assert((IsExtent(pid) && sync::PageState::LockState(v) != sync::PageState::EXCLUSIVE) ||
                   (!PageIsDirty(pid) && !IsExtent(pid)));
            to_evict.push_back(pid);
          }
          break;
        case sync::PageState::UNLOCKED: ps.TryMark(v); break;
        default: break;
      };
    });
  }

  // we can't do anything, return
  if (to_evict.size() + to_write.size() == 0) { return; }

  // 1. write dirty pages & mark them as clean
  for (auto &pid : to_write) {
    if (!IsExtent(pid)) { frame_[pid].last_written_gsn = ToPtr(pid)->p_gsn; }
  }
  aio_interface_[worker_thread_id].WritePages(to_write);
  statistics::buffer::write_cnt += to_write.size();

  // 2. try to X lock all clean page candidates
  std::erase_if(to_evict, [&](pageid_t pid) {
    sync::PageState &ps = GetPageState(pid);
    u64 v               = ps.StateAndVersion();
    return (sync::PageState::LockState(v) != sync::PageState::MARKED) || !ps.TryLockExclusive(v);
  });

  // 3. try to upgrade lock for dirty page candidates
  for (auto &pid : to_write) {
    sync::PageState &ps = GetPageState(pid);
    u64 v               = ps.StateAndVersion();
    if (ps.UpgradeLock(v)) {
      // only this evict thread uses this page, we can remove it from buffer pool
      to_evict.push_back(pid);
    } else {
      // others are using this page, so it is possible in hot path, thus we don't evict it from buffer pool
      ps.UnlockShared();
    }
  }

  // 4. remove from page table
  u64 evict_size = 0;
  Ensure(to_evict.size() <= EXMAP_USER_INTERFACE_PAGES);
  for (size_t idx = 0; idx < to_evict.size(); idx++) {
    auto pid      = to_evict[idx];
    auto pg_count = ExtentPageCount(pid);
    evict_size    = evict_size + pg_count;

    // For normal buffer pool, simulate lookup here
    if (FLAGS_blob_normal_buffer_pool) {
      for (auto idx = 0UL; idx < pg_count; idx++) { resident_set_.Contain(pid + idx); }
    }

    exmap_interface_[worker_thread_id]->iov[idx].page = pid;
    exmap_interface_[worker_thread_id]->iov[idx].len  = pg_count;
  }
  if (ExmapAction(exmapfd_, EXMAP_OP_FREE, to_evict.size()) < 0) {
    throw leanstore::ex::GenericException("ioctl: EXMAP_OP_FREE error");
  }

  // 5. remove from hash table and unlock
  for (auto &pid : to_evict) {
    bool ret = resident_set_.Remove(pid);
    if (!ret) {
      throw leanstore::ex::GenericException(fmt::format("Evict page {} but it not reside in Buffer Pool", pid));
    }
    GetPageState(pid).UnlockExclusiveAndEvict();
  }

  statistics::buffer::evict_cnt += evict_size;
  physical_used_cnt_ -= evict_size;
}

// ------------------------------------------------------------------------------------------------------

/**
 * @brief Try to find a reusable free extent in the Free storage manager
 */
auto BufferManager::TryReuseExtent(u64 page_cnt, pageid_t &out_start_pid) -> bool {
  if (!enable_extent_tier_) { return false; }

  bool found_ext_was_evicted = false;

  auto found_range = free_storage_->RequestFreeExtent(
    page_cnt,
    [&](pageid_t pid) {
      auto &ps = GetPageState(pid);
      u64 v    = ps.StateAndVersion().load();
      if (!ps.TryLockExclusive(v)) { return false; }
      if (sync::PageState::LockState(v) == sync::PageState::EVICTED) { found_ext_was_evicted = true; }
      return true;
    },
    out_start_pid);

  if (found_range) {
    if (found_ext_was_evicted) {
      PAGE_FAULT(out_start_pid, page_cnt);
      PREPARE_EXTENT(out_start_pid, page_cnt);
    }
  }

  return found_range;
}

/**
 * @brief Allocate a single extent whose id is `extent_id`
 * `fixed_page_cnt` = 0 most of the time, and should only be set for Blob's TailExtent
 */
auto BufferManager::AllocExtent(extidx_t tier) -> pageid_t {
  pageid_t start_pid;
  u64 required_page_cnt = ExtentList::ExtentSize(tier);

  // Try reuse free extent, or allocate a new one if no free extent found
  auto found_range = TryReuseExtent(required_page_cnt, start_pid);
  if (!found_range) {
    /**
     * @brief Two designs, tiering vs randomly pick on a large area, leads to different extent alloc strategies
     * 1. tiering:
     *  - pros: efficient extent recycling & allocation
     *  - cons: consume large virtual memory addresses & pte overhead
     * 2. randomly pick on a large virtual memory area
     *  - pros: expensive PID interval management
     *  - cons: much cheaper pte overhead
     */
    if (enable_extent_tier_) {
      auto tier_pid = tier_alloc_[tier].fetch_add(ExtentList::ExtentSize(tier), std::memory_order_relaxed);
      Ensure(tier_pid < virtual_cnt_);
      start_pid = tier_pid + base_pid_ + tier * virtual_cnt_;
    } else {
      Ensure(extent_virtual_cnt_ > required_page_cnt);

      auto tries = 0;
      while (true) {
        tries++;
        start_pid    = RandomGenerator::GetRandU64(base_pid_, base_pid_ + extent_virtual_cnt_ - required_page_cnt);
        auto success = free_storage_->TryLockRange(start_pid, required_page_cnt);
        if (success) { break; }
      }
      if (start_profiling) { statistics::blob::blob_alloc_try += tries; }
    }

    // Prepare run-time env for new allocated extent
    NEW_ALLOC_PAGE(start_pid);
    PAGE_FAULT(start_pid, required_page_cnt);
    PREPARE_EXTENT(start_pid, required_page_cnt);
  }

  return start_pid;
}

/**
 * @brief Prepare necessary env for the extent eviction later
 *
 * The caller must ensure that `start_pid` is the start PID of an actual Extent
 */
void BufferManager::PrepareExtentEviction(pageid_t start_pid) {
  frame_[start_pid].prevent_evict = true;
  UnfixExclusive(start_pid);
}

/**
 * @brief Evict an extent from the buffer manager
 * The caller is responsible for ensuring that [start_pid..start_pid+page_cnt) should refer to an existing extent
 */
void BufferManager::EvictExtent(pageid_t pid) {
  /**
   * It's possible that this Extent was already evicted in a batch
   * The scenario is:
   *  Txn A commits Extent X -> Txn B remove X -> Txn C reuse X and commits -> Group Commit flush (A, B, C)
   * In this scenario, X will be EvictExtent() twice
   * Because a single EvictExtent() is enough to commit all A, B, and C, we shouldn't EvictExtent() 1 more time
   */
  if (frame_[pid].prevent_evict.load()) {
    frame_[pid].prevent_evict = false;

    auto &ps = GetPageState(pid);
    u64 v    = ps.StateAndVersion().load();
    if (ps.TryLockExclusive(v)) { ps.UnlockExclusive(); }
  }
}

/**
 * @brief Try to read one or multiple extents from disk, and set necessary run-time variables
 * The caller is responsible for ensuring that large_pages should all refer to existing extents
 */
void BufferManager::ReadExtents(const storage::LargePageList &large_pages) {
  u64 total_page_cnt = 0;
  storage::LargePageList to_read_lp;
  to_read_lp.reserve(large_pages.size());

  // 1. Acquire necessary lock & space for the large pages before reading them
  for (const auto &lp : large_pages) {
    auto require_read = FixShareImpl(lp.start_pid);
    if (require_read) {
      to_read_lp.emplace_back(lp.start_pid, lp.page_cnt);
      total_page_cnt += lp.page_cnt;
    }
    Ensure((sync::PageState::UNLOCKED < GetPageState(lp.start_pid).LockState()) &&
           (GetPageState(lp.start_pid).LockState() <= sync::PageState::EXCLUSIVE));
  }
  Ensure(to_read_lp.size() <= EXMAP_USER_INTERFACE_PAGES);
  physical_used_cnt_ += total_page_cnt;
  EnsureFreePages();

  // 2. Exmap calloc/prefault these huge pages + Read them with io_uring
  for (size_t idx = 0; idx < to_read_lp.size(); idx++) {
    exmap_interface_[worker_thread_id]->iov[idx].page = to_read_lp[idx].start_pid;
    /**
     * @brief It's possible that this large page is a tail extent, i.e. it borrows a virtual large page from a tier
     *  Therefore, we should allocate the correct page count for that tier instead of relying on the provided page cnt
     */
    auto expected_page_cnt = ExtentPageCount(to_read_lp[idx].start_pid);
    Ensure(expected_page_cnt >= to_read_lp[idx].page_cnt);
    exmap_interface_[worker_thread_id]->iov[idx].len = expected_page_cnt;
  }
  Ensure(ExmapAction(exmapfd_, EXMAP_OP_ALLOC, to_read_lp.size()) >= 0);
  aio_interface_[worker_thread_id].ReadLargePages(to_read_lp);

  // Mark all huge pages as Blob, and set them to SHARED state
  for (const auto &[start_pid, page_cnt] : to_read_lp) {
    resident_set_.Insert(start_pid);
    GetPageState(start_pid).DowngradeLock();
  }

  // 3. If normal buffer pool, lookup all normal pages
  if (FLAGS_blob_normal_buffer_pool) {
    for (const auto &lp : large_pages) {
      for (auto pid = lp.start_pid; pid < lp.start_pid + lp.page_cnt; pid++) { resident_set_.Contain(pid); }
    }
  }

  // Update statistics
  statistics::buffer::read_cnt += total_page_cnt;
}

auto BufferManager::ExtentPageCount(pageid_t pid) -> u64 {
  if (pid < virtual_cnt_) { return 1; }
  return (enable_extent_tier_) ? ExtentList::ExtentSize((pid - base_pid_) / virtual_cnt_) : frame_[pid].extent_size;
}

// ------------------------------------------------------------------------------------------------------

/**
 * @brief Acquire necessary Locks on huge page (either X or S) before reading that page
 */
auto BufferManager::FixShareImpl(pageid_t page_id) -> bool {
  auto &ps = GetPageState(page_id);
  for (auto cnt = 0;; cnt++) {
    u64 v = ps.StateAndVersion().load();
    switch (sync::PageState::LockState(v)) {
      case sync::PageState::EXCLUSIVE: {
        break;
      }
      case sync::PageState::EVICTED: {
        if (ps.TryLockExclusive(v)) { return true; }
        break;
      }
      default:
        if (ps.TryLockShared(v)) { return false; }
        break;
    }
    AsmYield(cnt);
  }
}

auto BufferManager::FixExclusive(pageid_t page_id) -> storage::Page * {
  ValidatePID(page_id);

  sync::PageState &ps = GetPageState(page_id);
  for (auto cnt = 0;; cnt++) {
    u64 v = ps.StateAndVersion().load();
    switch (sync::PageState::LockState(v)) {
      case sync::PageState::EVICTED: {
        if (ps.TryLockExclusive(v)) {
          HandlePageFault(page_id);
          return ToPtr(page_id);
        }
        break;
      }
      case sync::PageState::MARKED:
      case sync::PageState::UNLOCKED:
        if (ps.TryLockExclusive(v)) { return ToPtr(page_id); }
        break;
      default: break;
    }
    AsmYield(cnt);
  }
}

auto BufferManager::FixShare(pageid_t page_id) -> storage::Page * {
  ValidatePID(page_id);

  auto required_read = FixShareImpl(page_id);
  if (required_read) {
    HandlePageFault(page_id);
    GetPageState(page_id).DowngradeLock();
  }
  return ToPtr(page_id);
}

void BufferManager::UnfixExclusive(pageid_t page_id) {
  ValidatePID(page_id);
  GetPageState(page_id).UnlockExclusive();
}

void BufferManager::UnfixShare(pageid_t page_id) {
  ValidatePID(page_id);
  GetPageState(page_id).UnlockShared();
}

}  // namespace leanstore::buffer
