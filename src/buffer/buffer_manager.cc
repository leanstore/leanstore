#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/format.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "storage/aio.h"
#include "storage/blob/blob_state.h"

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

// Set the first ((i) % NO_BLOCKS_PER_LOCK) bits
#define SET_BITS(i) ((1UL << ((i) % NO_BLOCKS_PER_LOCK)) - 1)

#define NEW_ALLOC_PAGE(pid)                     \
  ({                                            \
    auto &ps     = GetPageState(pid);           \
    u64 v        = ps.StateAndVersion().load(); \
    bool success = ps.TryLockExclusive(v);      \
    Ensure(success);                            \
  })

#define EXTENT_FRAME_SET(pid, size)              \
  ({                                             \
    frame_[pid].Reset();                         \
    frame_[pid].SetFlag(PageFlagIdx::IS_EXTENT); \
    frame_[pid].evict_pg_count = (size);         \
  })

#define PAGE_FAULT(pid, page_cnt, new_alloc, auto_init_env)              \
  ({                                                                     \
    physical_used_cnt_ += (page_cnt);                                    \
    EnsureFreePages();                                                   \
    if (new_alloc) {                                                     \
      (pid) = alloc_cnt_.fetch_add(page_cnt, std::memory_order_relaxed); \
      ValidatePID(pid);                                                  \
      if (auto_init_env) {                                               \
        NEW_ALLOC_PAGE(pid);                                             \
        resident_set_.Insert(pid);                                       \
        frame_[pid].Reset();                                             \
      }                                                                  \
    }                                                                    \
    ExmapAlloc(pid, page_cnt);                                           \
  })

// For your info, please check virtual_alloc_size's description in BufferManager::Construction()
#define ALIAS_LOCAL_PTR_START(w_id) (virtual_cnt_ + 1 + (w_id)*alias_pg_cnt_)
#define ALIAS_AREA_CAPABLE(index, w_id) ((index) < ALIAS_LOCAL_PTR_START((w_id) + 1))

using ExtentList = leanstore::storage::ExtentList;

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

BufferManager::BufferManager(std::atomic<bool> &keep_running, storage::FreePageManager *fp)
    : virtual_size_(FLAGS_bm_virtual_gb * GB),
      physical_size_(FLAGS_bm_physical_gb * GB),
      alias_size_(FLAGS_bm_wl_alias_mb * MB),
      virtual_cnt_(virtual_size_ / PAGE_SIZE),
      physical_cnt_(physical_size_ / PAGE_SIZE),
      alias_pg_cnt_(alias_size_ / PAGE_SIZE),
      evict_batch_(FLAGS_bm_evict_batch_size),
      keep_running_(&keep_running),
      free_pages_(fp),
      page_state_(static_cast<sync::PageState *>(AllocHuge(virtual_cnt_ * sizeof(sync::PageState)))),
      resident_set_(physical_cnt_, page_state_),
      shalas_no_blocks_(virtual_cnt_ / alias_pg_cnt_),
      shalas_no_locks_(std::ceil(static_cast<float>(shalas_no_blocks_) / NO_BLOCKS_PER_LOCK)) {
  Construction();
}

BufferManager::BufferManager(u64 virtual_page_count, u64 physical_page_count, u64 extra_page_count, u64 evict_size,
                             std::atomic<bool> &keep_running, storage::FreePageManager *fp)
    : virtual_size_(virtual_page_count * PAGE_SIZE),
      physical_size_(physical_page_count * PAGE_SIZE),
      alias_size_(extra_page_count * PAGE_SIZE),
      virtual_cnt_(virtual_page_count),
      physical_cnt_(physical_page_count),
      alias_pg_cnt_(extra_page_count),
      evict_batch_(evict_size),
      keep_running_(&keep_running),
      free_pages_(fp),
      page_state_(static_cast<sync::PageState *>(AllocHuge(virtual_cnt_ * sizeof(sync::PageState)))),
      resident_set_(physical_cnt_, page_state_),
      shalas_no_blocks_(virtual_cnt_ / alias_pg_cnt_),
      shalas_no_locks_(std::ceil(static_cast<float>(shalas_no_blocks_) / NO_BLOCKS_PER_LOCK)) {
  Construction();
}

BufferManager::~BufferManager() {
  close(exmapfd_);
  close(blockfd_);
}

void BufferManager::Construction() {
  assert(virtual_size_ >= physical_size_);
  blockfd_ = open(FLAGS_db_path.c_str(), O_RDWR | O_DIRECT, S_IRWXU);
  Ensure(blockfd_ > 0);

  /**
   * @brief Beside the actuall buffer pool (i.e. virtual_mem_[0 .. virtual_size_]),
   *    we allocate extra virtual memory area per worker for Blob operation (e.g., PageAliasGuard())
   *
   * Acronyms and usage:
   * - bm:      the buffer pool manager (vmcache)
   * - exp:     extra page, to prevent segfaults during optimistic reads
   * - wl:      worker-local aliasing area
   * - shalas:  shared-area for aliasing, should be as big as the main buffer pool
   *
   * The virtual memory area, therefore, looks like the following figure:
   *  |-------------------------|-----|----|----|----|----|------------------------|
   *  | Main bm (virtual_size_) | exp | wl | wl |....| wl | shalas (virtual_size_) |
   *  |-------------------------|-----|----|----|----|----|------------------------|
   *
   * Note that all ptrs belong to those `wl` and `shalas` areas are considered INVALID by all components except
   * BlobManager All of the addresses should be PAGE_SIZE aligned
   */
  u64 virtual_alloc_size = virtual_size_ + PAGE_SIZE + alias_size_ * FLAGS_worker_count + virtual_size_;

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
  /**
   * @brief Pinning worker + clearing local TLB cache only improves multi-threading performance
   * For single-thread env, actually TLB shootdown is slightly faster
   */
  buffer.flags = (FLAGS_worker_pin_thread) ? exmap_flags::EXMAP_CPU_AFFINITY : 0;
  auto ret     = ioctl(exmapfd_, EXMAP_IOCTL_SETUP, &buffer);
  if (ret < 0) { throw leanstore::ex::GenericException("ioctl: exmap_setup error"); }

  exmap_interface_.resize(no_interfaces);
  for (size_t idx = 0; idx < no_interfaces; idx++) {
    exmap_interface_[idx] = static_cast<exmap_user_interface *>(
      mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, exmapfd_, EXMAP_OFF_INTERFACE(idx)));
    if (exmap_interface_[idx] == MAP_FAILED) { throw leanstore::ex::GenericException("Setup exmap_interface_ error"); };
  }

  // Setup virtual mem on top of exmap
  virtual_mem_ =
    static_cast<storage::Page *>(mmap(nullptr, virtual_alloc_size, PROT_READ | PROT_WRITE, MAP_SHARED, exmapfd_, 0));
  if (virtual_mem_ == MAP_FAILED) { throw leanstore::ex::GenericException("mmap failed"); };

  // Setup aliasing areas
  wl_alias_ptr_.resize(FLAGS_worker_count);
  for (size_t idx = 0; idx < FLAGS_worker_count; idx++) { wl_alias_ptr_[idx] = ALIAS_LOCAL_PTR_START(idx); }
  Ensure(virtual_cnt_ % alias_pg_cnt_ == 0);
  shalas_ptr_  = 0;
  shalas_area_ = &virtual_mem_[ALIAS_LOCAL_PTR_START(FLAGS_worker_count)];
  shalas_lk_   = std::make_unique<std::atomic<u64>[]>(shalas_no_locks_);
  shalas_lk_acquired_.resize(FLAGS_worker_count);

  // Init page_state and buffer frames
  for (size_t idx = 0; idx < virtual_cnt_; idx++) { page_state_[idx].Init(); }
  frame_ = static_cast<buffer::BufferFrame *>(AllocHuge(virtual_cnt_ * sizeof(buffer::BufferFrame)));

  // Construct AIO interfaces for writing pages
  aio_interface_.reserve(no_interfaces);
  for (size_t idx = 0; idx < no_interfaces; idx++) { aio_interface_.emplace_back(blockfd_, virtual_mem_); }

  physical_used_cnt_ = 0;
  alloc_cnt_         = 0;

  LOG_INFO("VMCache: Path(%s), VirtualGB(%lu), VirtualCount(%lu), PhysGB(%lu), PhysCount(%lu), EvictSize(%lu)",
           FLAGS_db_path.c_str(), virtual_size_ / GB, virtual_cnt_, physical_size_ / GB, physical_cnt_, evict_batch_);
}

// ------------------------------------------------------------------------------------------------------

void BufferManager::AllocMetadataPage() {
  // Alloc page 0 for metadata
  AllocPage();
  GetPageState(0).UnlockExclusive();
}

auto BufferManager::GetWalInfo() -> std::pair<int, u64> { return std::make_pair(blockfd_, virtual_size_); }

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

// ------------------------------------------------------------------------------------------------------
auto BufferManager::GetFreePageManager() -> storage::FreePageManager * { return free_pages_; }

// ------------------------------------------------------------------------------------------------------

auto BufferManager::GetPageState(pageid_t page_id) -> sync::PageState & { return page_state_[page_id]; }

void BufferManager::ValidatePID(pageid_t pid) {
  if (pid >= virtual_cnt_) { throw std::runtime_error("Page id " + std::to_string(pid) + " is invalid"); }
}

auto BufferManager::IsValidPtr(void *page) -> bool {
  return (page >= virtual_mem_) && (page < (virtual_mem_ + virtual_size_ + 16));
}

auto BufferManager::ToPID(void *page) -> pageid_t { return reinterpret_cast<storage::Page *>(page) - virtual_mem_; }

auto BufferManager::ToPtr(pageid_t page_id) -> storage::Page * {
  if (FLAGS_blob_normal_buffer_pool) { resident_set_.Contain(page_id); }
  return &virtual_mem_[page_id];
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

void BufferManager::EnsureFreePages() {
  if (FLAGS_page_provider_thread > 0) {
    // Page Provider thread is enabled, then we should sleep to wait for free pages
    while ((physical_used_cnt_ >= physical_cnt_ * 0.95) && (keep_running_->load())) { AsmYield(); }
    return;
  }
  while ((physical_used_cnt_ >= physical_cnt_ * 0.9) && (keep_running_->load())) { Evict(); }
}

void BufferManager::ExmapAlloc(pageid_t pid, size_t mem_alloc_sz) {
  // TODO(Duy): Support ExmapAlloc with `mem_alloc_sz > EXMAP_PAGE_MAX_PAGES * EXMAP_USER_INTERFACE_PAGES`
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

auto BufferManager::AllocPage() -> storage::Page * {
  // Find a free page before allocating at the end
  pageid_t pid;
  auto found_page = TryReuseExtent(1, false, pid);

  if (!found_page) { PAGE_FAULT(pid, 1, true, true); }
  virtual_mem_[pid].dirty = true;
  return ToPtr(pid);
}

/**
 * @brief Try to ensure that there are free frames (i.e. virtual mem in vmcache) in
 *          the buffer manager.
 *        After that, read the page and load it into the buffer pool
 */
void BufferManager::HandlePageFault(pageid_t page_id) {
  assert(GetPageState(page_id).LockState() == sync::PageStateMode::EXCLUSIVE);
  physical_used_cnt_++;
  EnsureFreePages();
  ReadPage(page_id);
  resident_set_.Insert(page_id);
  frame_[page_id].Reset();
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
      auto page = ToPtr(pid);
      auto &ps  = GetPageState(pid);
      u64 v     = ps.StateAndVersion();

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
            if (rand() % maximum_page_size_.load() > frame_[pid].evict_pg_count) { return; }
          }
          /**
           * @brief Add this page to to_write if
           * - This page is part of a BLOB/extent, and it is marked to EXTENT_TO_WRITE
           * - This page is a normal page (not a BLOB page) and dirty
           */
          if ((frame_[pid].IsExtent() && frame_[pid].ToWriteExtent()) || (!frame_[pid].IsExtent() && page->dirty)) {
            /**
             * @brief 2 conditions:
             * - prevent normal pages to be written to disk before its lastest log has been flushed
             *    (note that blob pages don't associate with any log record, hence this condition doesn't apply)
             * - prevents the page from entering (EXCLUSIVE, MARKED, EVICTED) state
             */
            if ((frame_[pid].IsExtent() || page->p_gsn <= flushed_log_gsn) && (ps.TryLockShared(v))) {
              to_write.push_back(pid);
            }
          } else {
            /**
             * @brief In the case of blob page, eviction reaches this stage only when the blob content is flushed
             * That is, if the blob is not flushed yet, its PageState should be EXCLUSIVE,
             *  and all EXCLUSIVE pages can't be moved to any other state before being moved to UNLOCKED
             */
            assert((frame_[pid].IsExtent() && sync::PageState::LockState(v) != sync::PageState::EXCLUSIVE) ||
                   (!ToPtr(pid)->dirty && !frame_[pid].IsExtent()));
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
    if (!frame_[pid].IsExtent()) { ToPtr(pid)->dirty = false; }
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
    auto pid   = to_evict[idx];
    evict_size = evict_size + frame_[pid].evict_pg_count;

    // For normal buffer pool, simulate lookup here
    for (auto idx = 0UL; idx < frame_[pid].evict_pg_count; idx++) { resident_set_.Contain(pid + idx); }

    exmap_interface_[worker_thread_id]->iov[idx].page = pid;
    exmap_interface_[worker_thread_id]->iov[idx].len  = frame_[pid].evict_pg_count;
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
 * @brief Try to find a reusable free extent in the Free Page manager
 * If there is one + that found extent is larger than the asked size,
 *  split that extent into multiple smaller extents and set necessary env
 */
auto BufferManager::TryReuseExtent(u64 required_page_cnt, bool is_special_block, pageid_t &out_start_pid) -> bool {
  storage::TierList split_extents;
  bool found_ext_was_evicted = false;

  auto found_range = free_pages_->RequestFreeExtent(
    required_page_cnt,
    [&](pageid_t pid) {
      auto &ps = GetPageState(pid);
      u64 v    = ps.StateAndVersion().load();
      if (!ps.TryLockExclusive(v)) { return false; }
      if (sync::PageState::LockState(v) == sync::PageState::EVICTED) { found_ext_was_evicted = true; }
      return true;
    },
    out_start_pid, split_extents);

  /* TODO(Duy): The below is too complicated, should simplify it */
  if (found_range) {
    if (found_ext_was_evicted) {
      PAGE_FAULT(out_start_pid, required_page_cnt, false, false);
      if (is_special_block) {
        PrepareTailExtent(true, out_start_pid, required_page_cnt);
      } else {
        PrepareExtentEnv(out_start_pid, required_page_cnt);
      }
    } else {
      // Resident Set should already contain this `out_start_pid`
      if (is_special_block) {
        storage::TailExtent::SplitToExtents(out_start_pid, required_page_cnt, [&](pageid_t pid, extidx_t index) {
          if (pid != out_start_pid) {
            NEW_ALLOC_PAGE(pid);
            resident_set_.Insert(pid);
          }
          EXTENT_FRAME_SET(pid, ExtentList::ExtentSize(index));
        });
      } else {
        EXTENT_FRAME_SET(out_start_pid, required_page_cnt);
      }
      /**
       * If the splitted extents are already in memory, we need to:
       * - Acquire X-lock on them before modify their state, i.e. NEW_ALLOC_PAGE()
       * - Add them to resident_set and set the evict_pg_count, i.e. PrepareExtentEnv()
       * - Unlock X-lock to allow later transaction reuse them
       */
      for (auto &extent : split_extents) {
        NEW_ALLOC_PAGE(extent.start_pid);
        PrepareExtentEnv(extent.start_pid, ExtentList::ExtentSize(extent.tier_index));
        UnfixExclusive(extent.start_pid);
      }
    }
  }

  return found_range;
}

/**
 * @brief Allocate a single extent whose id is `extent_id`
 * `fixed_page_cnt` = 0 most of the time, and should only be set for Blob's TailExtent
 */
auto BufferManager::AllocExtent(extidx_t extent_idx, u64 fixed_page_cnt) -> pageid_t {
  Ensure(fixed_page_cnt <= ExtentList::ExtentSize(extent_idx));
  auto is_special_block = fixed_page_cnt != 0;
  u64 required_page_cnt = is_special_block ? fixed_page_cnt : ExtentList::ExtentSize(extent_idx);

  // Alloc new extent
  pageid_t start_pid;
  auto found_range = TryReuseExtent(required_page_cnt, is_special_block, start_pid);

  if (!found_range) {
    Ensure(alloc_cnt_ + required_page_cnt < virtual_cnt_);
    PAGE_FAULT(start_pid, required_page_cnt, true, false);

    // Determine whether we need to prepare env for all split extents or a single one
    if (!is_special_block) {
      NEW_ALLOC_PAGE(start_pid);
      PrepareExtentEnv(start_pid, ExtentList::ExtentSize(extent_idx));
    } else {
      Ensure(fixed_page_cnt < ExtentList::ExtentSize(extent_idx));
      PrepareTailExtent(false, start_pid, fixed_page_cnt);
    }
  }

  return start_pid;
}

void BufferManager::PrepareExtentEnv(pageid_t start_pid, u64 page_cnt) {
  assert(ExtentList::TierIndex(page_cnt, true) < ExtentList::NO_TIERS);
  resident_set_.Insert(start_pid);
  EXTENT_FRAME_SET(start_pid, page_cnt);
  if (FLAGS_bm_enable_fair_eviction) { UpdateMax(maximum_page_size_, page_cnt); }
}

void BufferManager::PrepareTailExtent(bool already_lock_1st_extent, pageid_t start_pid, u64 page_cnt) {
  storage::TailExtent::SplitToExtents(start_pid, page_cnt, [&](pageid_t pid, extidx_t index) {
    if (pid != start_pid || !already_lock_1st_extent) { NEW_ALLOC_PAGE(pid); }
    PrepareExtentEnv(pid, ExtentList::ExtentSize(index));
  });
}

/**
 * @brief Prepare necessary env for the extent eviction later
 *
 * The caller must ensure that `start_pid` is the start PID of an actual Extent
 */
void BufferManager::PrepareExtentEviction(pageid_t start_pid) {
  if (FLAGS_blob_logging_variant < 0) {
    frame_[start_pid].SetFlag(PageFlagIdx::EXTENT_TO_WRITE);
  } else {
    frame_[start_pid].prevent_evict = true;
  }
  UnfixExclusive(start_pid);
}

// ------------------------------------------------------------------------------------------------------

/**
 * @brief Evict an extent from the buffer manager
 * The caller is responsible for ensuring that [start_pid..start_pid+page_cnt) should refer to an existing extent
 */
void BufferManager::EvictExtent(pageid_t pid, u64 page_cnt) {
  assert(FLAGS_blob_logging_variant >= 0);

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
    if (ps.TryLockExclusive(v)) {
      switch (FLAGS_blob_logging_variant) {
        case 0:
          exmap_interface_[worker_thread_id]->iov[0] = {.page = pid, .len = page_cnt};
          if (ExmapAction(exmapfd_, EXMAP_OP_FREE, 1) < 0) {
            throw leanstore::ex::GenericException("ioctl: EXMAP_OP_FREE error");
          }
          resident_set_.Remove(pid);
          physical_used_cnt_ -= page_cnt;
          statistics::buffer::evict_cnt += page_cnt;
          ps.UnlockExclusiveAndEvict();
          break;
        case 1: ps.UnlockExclusive(); break;
        case 2: ps.UnlockExclusiveAndMark(); break;
        default: UnreachableCode(); break;
      }
    }
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
    exmap_interface_[worker_thread_id]->iov[idx].len  = to_read_lp[idx].page_cnt;
  }
  Ensure(ExmapAction(exmapfd_, EXMAP_OP_ALLOC, to_read_lp.size()) >= 0);
  aio_interface_[worker_thread_id].ReadLargePages(to_read_lp);

  // Mark all huge pages as Blob, and set them to SHARED state
  for (const auto &[start_pid, page_cnt] : to_read_lp) {
    resident_set_.Insert(start_pid);
    EXTENT_FRAME_SET(start_pid, page_cnt);
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

// ------------------------------------------------------------------------------------------------------

/**
 * @brief Either Set or Clear bits in [block_start..block_end] in shalas_lk_
 */
auto BufferManager::ToggleShalasLocks(bool set_op, u64 &block_start, u64 block_end) -> bool {
  if (block_end > shalas_no_blocks_) { return false; }
  bool success = true;

  for (; block_start < block_end;) {
    // Detect the necessary bits for locking [pos..next_pos)
    auto next_pos = std::min(block_start + NO_BLOCKS_PER_LOCK - block_start % NO_BLOCKS_PER_LOCK, block_end);
    auto bits     = (next_pos % NO_BLOCKS_PER_LOCK == 0) ? std::numeric_limits<u64>::max() : SET_BITS(next_pos);
    if (block_start % NO_BLOCKS_PER_LOCK != 0) { bits -= SET_BITS(block_start); }

    // Try to acquire the necessary bits
    u64 old_lock;
    u64 x_lock;
    auto &lock = shalas_lk_[block_start / NO_BLOCKS_PER_LOCK];
    do {
      old_lock = lock.load();

      if (set_op) {
        x_lock = old_lock | bits;
        // If at least one of the bits was acquired by concurrent workers, abort
        if ((old_lock & bits) > 0) {
          success = false;
          break;
        }
      } else {
        x_lock = old_lock & ~bits;
      }
    } while (!lock.compare_exchange_strong(old_lock, x_lock));

    // The last lock acquisition fails, break to undo the prev lock acquisitions
    if (!success) { break; }

    // Move to next lock block
    block_start = next_pos;
  }

  return success;
}

auto BufferManager::RequestAliasingArea(u64 requested_size) -> pageid_t {
  u64 required_page_cnt = storage::blob::BlobState::PageCount(requested_size);

  // Check if the worker-local aliasing area is big enough for this request
  if (ALIAS_AREA_CAPABLE(wl_alias_ptr_[worker_thread_id] + required_page_cnt, worker_thread_id)) {
    auto start_pid = wl_alias_ptr_[worker_thread_id];
    wl_alias_ptr_[worker_thread_id] += required_page_cnt;
    return start_pid;
  }

  // There is no room left in worker-local aliasing area, fallback to shalas, i.e. shared-aliasing area
  u64 required_block_cnt = std::ceil(static_cast<float>(required_page_cnt) / alias_pg_cnt_);
  u64 pos;      // Inclusive
  u64 new_pos;  // Exclusive

  while (true) {
    // Try to find a batch which is large enough to store `required_page_cnt`,
    //  i.e. consecutive of blocks which is bigger than `required_block_cnt`
    bool found_range;
    do {
      pos         = shalas_ptr_.load();
      new_pos     = pos + required_block_cnt;
      found_range = new_pos <= shalas_no_blocks_;
      if (new_pos >= shalas_no_blocks_) { new_pos = 0; }
    } while (!shalas_ptr_.compare_exchange_strong(pos, new_pos));

    // The prev shalas_ptr_ is at the end of the shared-alias area, and there is no room left for the `requested_size`
    if (!found_range) { continue; }
    auto end_pos = (new_pos == 0) ? shalas_no_blocks_ : new_pos;
    Ensure(pos + required_block_cnt == end_pos);

    // Now try to acquire bits in [pos..pos + required_block_cnt)
    auto start_pos       = pos;
    auto acquire_success = ToggleShalasLocks(true, start_pos, end_pos);

    // Lock acquisition success, return the start_pid
    if (acquire_success) {
      shalas_lk_acquired_[worker_thread_id].emplace_back(pos, required_block_cnt);
      return ToPID(&shalas_area_[pos * alias_pg_cnt_]);
    }

    // Lock acquisition fail, undo bits [pos..start_pos), and continue trying to acquire a suitable range
    ToggleShalasLocks(false, pos, start_pos);
  }

  UnreachableCode();
  return 0;  // This is purely to silent the compiler/clang-tidy warning
}

void BufferManager::ReleaseAliasingArea() {
  ExmapAction(exmapfd_, EXMAP_OP_RM_SD, 0);
  if (wl_alias_ptr_[worker_thread_id] > ALIAS_LOCAL_PTR_START(worker_thread_id)) {
    wl_alias_ptr_[worker_thread_id] = ALIAS_LOCAL_PTR_START(worker_thread_id);
  }
  if (!shalas_lk_acquired_[worker_thread_id].empty()) {
    for (auto &[block_pos, block_cnt] : shalas_lk_acquired_[worker_thread_id]) {
      Ensure(ToggleShalasLocks(false, block_pos, block_pos + block_cnt));
    }
    shalas_lk_acquired_[worker_thread_id].clear();
  }
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
