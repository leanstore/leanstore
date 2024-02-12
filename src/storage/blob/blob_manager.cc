#include "storage/blob/blob_manager.h"
#include "transaction/transaction_manager.h"

#include <algorithm>
#include <span>

namespace leanstore::storage::blob {

// -------------------------------------------------------------------------------------
/**
 * @brief BlobManager's Private utilities
 */
#define ALLOCATE_REMAIN_CONTENT(idx, is_special_block)                                                 \
  ({                                                                                                   \
    auto remaining_bytes = std::min(ExtentList::ExtentSize(idx) * PAGE_SIZE, payload.size() - offset); \
    auto pid             = out_blob->extents.extent_pid[idx] =                                         \
      buffer_->AllocExtent((idx), (is_special_block) ? BlobState::PageCount(remaining_bytes) : 0);     \
    if (FLAGS_blob_normal_buffer_pool) {                                                               \
      buffer_->ChunkOperation(pid, remaining_bytes, [&](u64 off, std::span<u8> page_addr) {            \
        std::memcpy(page_addr.data(), payload.data() + offset + off, page_addr.size());                \
      });                                                                                              \
    } else {                                                                                           \
      std::memcpy(buffer_->ToPtr(pid), payload.data() + offset, remaining_bytes);                      \
    }                                                                                                  \
    offset += remaining_bytes;                                                                         \
  })

#define PREPARE_FREE_SPECIAL_BLK(blob)                                                           \
  ({                                                                                             \
    auto pid = (blob)->extents.special_blk.start_pid;                                            \
    (blob)->extents.special_blk.SplitToExtents([&](pageid_t start_pid, extidx_t index) {         \
      buffer_->GetFreePageManager()->PrepareFreeTier(start_pid, index);                          \
      pid += ExtentList::ExtentSize(index);                                                      \
    });                                                                                          \
    Ensure(pid == (blob)->extents.special_blk.start_pid + (blob)->extents.special_blk.page_cnt); \
  })

#define SHA2_CALC_LP(extent)                                                                      \
  ({                                                                                              \
    auto remaining_bytes = std::min(out_blob->blob_size - offset, (extent).page_cnt * PAGE_SIZE); \
    if (FLAGS_blob_normal_buffer_pool) {                                                          \
      buffer_->ChunkOperation((extent).start_pid, remaining_bytes, [&](u64, std::span<u8> page) { \
        BlobState::sha_context.Update(page.data(), page.size());                                  \
      });                                                                                         \
    } else {                                                                                      \
      BlobState::sha_context.Update(buffer_->ToPtr((extent).start_pid), remaining_bytes);         \
    }                                                                                             \
    offset += remaining_bytes;                                                                    \
  })

/**
 * @brief Expect the buffer manager to evict the whole extent at once,
 *  but only write pages in range [start_pid..start_pid+pg_cnt)
 *
 * Every large page of `out_to_write_lps` is either a portion or the whole extent
 *  of the corresponding extent in `out_to_evict_ets`
 * That is, assume the large page `lp = out_to_write_lps[i]` and the respective extent `ext = out_to_evict_ets[i]`
 *  then, `lp.start_pid >= ext.start_pid && lp.start_pid + lp.page_cnt <= ext.start_pid + ext.page_cnt`
 *
 * @param out_blob          The Blob Handler ptr
 * @param ext_id            The index of the extent in `out_blob->extents`
 * @param start_pid         The Start PID of the dirty pages, not Start PID of the extent
 * @param pg_cnt            Number of dirty pages, not size of the extent
 * @param out_to_write_lps  A vector containing all large pages to be written out in Group Commit
 * @param out_to_evict_ets  A vector containing all extents to be logically evicted by Buffer Manager
 */
#define MARK_EXTENT_EVICT(out_blob, ext_id, start_pid, pg_cnt, out_to_write_lps, out_to_evict_ets)            \
  ({                                                                                                          \
    pageid_t page_id = (out_blob)->extents.extent_pid[(ext_id)];                                              \
    Ensure((page_id <= (start_pid)) && ((start_pid) + (pg_cnt) <= page_id + ExtentList::ExtentSize(ext_id))); \
    (out_to_write_lps).emplace_back(start_pid, pg_cnt);                                                       \
    (out_to_evict_ets).emplace_back(page_id, ExtentList::ExtentSize(ext_id));                                 \
    buffer_->PrepareExtentEviction((out_blob)->extents.extent_pid[(ext_id)]);                                 \
  })

thread_local SHA256H BlobState::sha_context      = {};
thread_local BlobState *BlobManager::active_blob = nullptr;
thread_local roaring::Roaring64Map BlobManager::extent_loaded{};
thread_local std::array<u8, BlobState::MallocSize(ExtentList::EXTENT_CNT_MASK)> BlobManager::blob_handler_storage;

// -------------------------------------------------------------------------------------
PageAliasGuard::PageAliasGuard(buffer::BufferManager *buffer, const BlobState &blob, u64 required_load_size)
    : buffer_(buffer) {
  // FLAGS_blob_normal_buffer_pool: 2nd extra overhead
  if (FLAGS_blob_normal_buffer_pool) {
    ptr_       = reinterpret_cast<u8 *>(malloc(required_load_size));
    u64 offset = 0;
    size_t idx = 0;
    for (; (idx < blob.extents.NumberOfExtents()) && (offset < required_load_size); idx++) {
      auto copy_size = std::min(required_load_size - offset, ExtentList::ExtentSize(idx) * PAGE_SIZE);
      buffer->ChunkOperation(blob.extents.extent_pid[idx], copy_size, [&](u64 off, std::span<u8> payload) {
        std::memcpy(&ptr_[offset + off], payload.data(), payload.size());
      });
      offset += copy_size;
    }
    if (blob.extents.special_blk.in_used && offset < required_load_size) {
      Ensure(idx++ == blob.extents.NumberOfExtents());
      auto copy_size = std::min(required_load_size - offset, blob.extents.special_blk.page_cnt * PAGE_SIZE);
      buffer->ChunkOperation(blob.extents.special_blk.start_pid, copy_size, [&](u64 off, std::span<u8> payload) {
        std::memcpy(&ptr_[offset + off], payload.data(), payload.size());
      });
      offset += copy_size;
    }
    Ensure(offset >= required_load_size);
    return;
  }

  // Create Alias working area
  auto alias_pid = buffer->RequestAliasingArea(blob.blob_size);
  ptr_           = reinterpret_cast<u8 *>(buffer_->ToPtr(alias_pid));

  // Prepare the aliasing params
  u64 alias_size = 0;
  size_t idx     = 0;
  for (; (idx < blob.extents.NumberOfExtents()) && (alias_size < required_load_size); idx++) {
    auto pg_cnt = ExtentList::ExtentSize(idx);
    assert(pg_cnt <= EXMAP_PAGE_MAX_PAGES);
    buffer->exmap_interface_[worker_thread_id]->iov[idx].page = blob.extents.extent_pid[idx];
    buffer->exmap_interface_[worker_thread_id]->iov[idx].len  = pg_cnt;
    assert(buffer->exmap_interface_[worker_thread_id]->iov[idx].len == pg_cnt);
    alias_size += pg_cnt * PAGE_SIZE;
  }
  if (blob.extents.special_blk.in_used && alias_size < required_load_size) {
    idx++;
    assert(blob.extents.special_blk.page_cnt <= EXMAP_PAGE_MAX_PAGES);
    buffer->exmap_interface_[worker_thread_id]->iov[idx - 1].page = blob.extents.special_blk.start_pid;
    buffer->exmap_interface_[worker_thread_id]->iov[idx - 1].len  = blob.extents.special_blk.page_cnt;
    assert(buffer->exmap_interface_[worker_thread_id]->iov[idx - 1].page == blob.extents.special_blk.start_pid);
    alias_size += blob.extents.special_blk.page_cnt * PAGE_SIZE;
  }
  Ensure(alias_size >= required_load_size);

  // Aliasing the whole blob
  struct exmap_action_params params = {
    .interface = static_cast<u16>(worker_thread_id),
    .iov_len   = static_cast<u16>(idx),
    .opcode    = static_cast<u16>(EXMAP_OP_SHADOW),
    .page_id   = alias_pid,
  };

  // Execute Exmap SHADOW operation
  Ensure(ioctl(buffer->exmapfd_, EXMAP_IOCTL_ACTION, &params) >= 0);
}

PageAliasGuard::~PageAliasGuard() {
  if (FLAGS_blob_normal_buffer_pool) {
    free(ptr_);
  } else {
    buffer_->ReleaseAliasingArea();
  }
}

auto PageAliasGuard::GetPtr() -> u8 * { return ptr_; }

// -------------------------------------------------------------------------------------

BlobManager::BlobManager(buffer::BufferManager *buffer_manager) : buffer_(buffer_manager) {}

/**
 * @brief Set all extents of the given special blocks for eviction
 */
void BlobManager::MarkTailExtentForEviction(const TailExtent &special_blk, LargePageList &out_to_write_lps,
                                            LargePageList &out_to_evict_ets) {
  if (!special_blk.in_used) { return; }

  auto start_pid = special_blk.start_pid;
  for (; start_pid < special_blk.start_pid + special_blk.page_cnt;) {
    auto extent_size = buffer_->BufferFrame(start_pid).evict_pg_count;
    out_to_write_lps.emplace_back(start_pid, extent_size);
    out_to_evict_ets.emplace_back(start_pid, extent_size);
    buffer_->PrepareExtentEviction(start_pid);
    start_pid += extent_size;
  }

  Ensure(start_pid == special_blk.start_pid + special_blk.page_cnt);
}

void BlobManager::FreshBlobAllocation(std::span<const u8> payload, BlobState *out_blob, bool likely_grow,
                                      LargePageList &out_to_write_lps, LargePageList &out_to_evict_ets) {
  auto required_page_cnt   = BlobState::PageCount(payload.size());
  auto required_no_extents = ExtentList::NoSpanExtents(required_page_cnt);
  Ensure(required_no_extents > 0);

  // If the payload fits 100% into our tiering allocation, there will be no difference between
  //  fixed-size blob allocation vs grow-likely blob allocation
  if (required_page_cnt == ExtentList::TotalSizeExtents(required_no_extents - 1)) { likely_grow = true; }

  // Update BlobState output
  out_blob->blob_size          = payload.size();
  out_blob->extents.extent_cnt = required_no_extents;
  std::memcpy(out_blob->blob_prefix, payload.data(), BlobState::PREFIX_LENGTH);

  // Allocate extents one by one
  u64 offset = 0;
  for (u8 idx = 0; idx < required_no_extents; idx++) {
    ALLOCATE_REMAIN_CONTENT(idx, (!likely_grow) && (idx == required_no_extents - 1));
  }

  // If this is a fixed allocation, reduce extent_cnt and set the corresponding `special_blk`
  if (!likely_grow) {
    // If required_no_extents == 1, then required_page_cnt == ExtentList::TotalSizeExtents(required_no_extents - 1)
    //  i.e. likely_grow will be reset to true at the start of this function
    Ensure(required_no_extents > 1);
    auto last_blk_size = required_page_cnt - ExtentList::TotalSizeExtents(required_no_extents - 2);
    Ensure((last_blk_size >> 20) == 0);
    out_blob->extents.special_blk = {
      .start_pid = (out_blob)->extents.extent_pid[required_no_extents - 1],
      .page_cnt  = static_cast<u32>(last_blk_size),
      .in_used   = true,
    };
    out_blob->extents.extent_cnt--;
  }

  // Mark all extents (& special blk) for eviction
  u64 remaining_pages = BlobState::PageCount(payload.size());
  for (size_t idx = 0; idx < out_blob->extents.extent_cnt; idx++) {
    auto pid = out_blob->extents.extent_pid[idx];
    MARK_EXTENT_EVICT(out_blob, idx, pid, std::min(ExtentList::ExtentSize(idx), remaining_pages), out_to_write_lps,
                      out_to_evict_ets);
    remaining_pages -= ExtentList::ExtentSize(idx);
  }
  MarkTailExtentForEviction(out_blob->extents.special_blk, out_to_write_lps, out_to_evict_ets);
}

void BlobManager::ExtendExistingBlob(std::span<const u8> payload, BlobState *out_blob, LargePageList &out_to_write_lps,
                                     LargePageList &out_to_evict_ets) {
  // Calculate number of extents to be allocated
  auto prev_no_extents     = ExtentList::NoSpanExtents(out_blob->PageCount());
  auto required_page_cnt   = BlobState::PageCount(out_blob->blob_size + payload.size());
  auto required_no_extents = ExtentList::NoSpanExtents(required_page_cnt);
  Ensure(required_no_extents > 0);

  // Update latest data
  out_blob->blob_size += payload.size();
  out_blob->extents.extent_cnt = required_no_extents;

  // Mark all extents for eviction
  u64 offset = 0;
  for (auto idx = prev_no_extents; idx < required_no_extents; idx++) {
    ALLOCATE_REMAIN_CONTENT(idx, false);
    MARK_EXTENT_EVICT(out_blob, idx, static_cast<pageid_t>(out_blob->extents.extent_pid[idx]),
                      ExtentList::ExtentSize(idx), out_to_write_lps, out_to_evict_ets);
  }
}

/**
 * @brief Load the full content of the corresponding BlobState
 */
void BlobManager::LoadBlobContent(const BlobState *blob, u64 required_load_size) {
  // Try to load all extents until meets the requirement
  u64 load_size = 0;
  LargePageList to_read_extents;
  for (auto &extent : blob->extents) {
    if (!extent_loaded.contains(extent.start_pid)) {
      extent_loaded.add(extent.start_pid);
      to_read_extents.emplace_back(extent.start_pid, extent.page_cnt);
    }
    load_size += extent.page_cnt * PAGE_SIZE;
    if (load_size >= required_load_size) { break; }
  }

  // If the load size still not meet the requirement, then we load the special block
  if (load_size < required_load_size) {
    Ensure(blob->extents.special_blk.in_used);
    blob->extents.special_blk.SplitToExtents([&](pageid_t start_pid, extidx_t index) {
      extent_loaded.add(start_pid);
      to_read_extents.emplace_back(start_pid, ExtentList::ExtentSize(index));
    });
    Ensure(load_size + blob->extents.special_blk.page_cnt * PAGE_SIZE >= required_load_size);
  }

  // Trigger the necessary read
  if (!to_read_extents.empty()) { buffer_->ReadExtents(to_read_extents); }
}

/**
 * @brief If the last extent of a Blob has free space,
 *  then write data from `payload` to that and mark those pages for eviction
 */
auto BlobManager::WriteNewDataToLastExtent(transaction::Transaction &txn, std::span<const u8> payload, BlobState *blob)
  -> u64 {
  u64 write_size        = 0;
  u64 remain_free_bytes = blob->RemainBytesInLastExtent();

  if (remain_free_bytes > 0) {
    // If the new payload fits into remaining space of the last extent, write it there
    auto last_idx      = blob->extents.NumberOfExtents() - 1;
    auto last_alloc_sz = ExtentList::ExtentSize(last_idx) * PAGE_SIZE;

    // Upgrade Lock of the last Extent from SHARED -> EXCLUSIVE, and remove it from the `extent_loaded` state
    Ensure(extent_loaded.contains(blob->extents.extent_pid[last_idx]));
    extent_loaded.remove(blob->extents.extent_pid[last_idx]);
    buffer_->GetPageState(blob->extents.extent_pid[last_idx]).ForceUpgradeLock();

    /**
     * @brief Determine the dirty pid, calculate by X + Y - Z, where the three variables are:
     * - X: Start PID of last extent
     * - Y: Size in Page of the last extent
     * - Z: Upper int bound of the (remaining free space / PAGE_SIZE)
     */
    auto pid = blob->extents.extent_pid[last_idx] + ExtentList::ExtentSize(last_idx) -
               static_cast<u64>(std::ceil(static_cast<float>(remain_free_bytes) / PAGE_SIZE));

    // The last allocated page of prev_blob is not fully loaded,
    //  so append that page to WAL and add some new content to that page
    if (remain_free_bytes % PAGE_SIZE > 0) {
      auto &log_entry = txn.LogWorker().ReservePageImageLog(PAGE_SIZE - (remain_free_bytes % PAGE_SIZE), pid);
      std::memcpy(log_entry.payload, buffer_->ToPtr(pid), PAGE_SIZE - (remain_free_bytes % PAGE_SIZE));
      txn.LogWorker().SubmitActiveLogEntry();
    }

    // Only write to the last remaining free bytes of the last extent and mark the Extent for evict
    // TODO(Duy): Add overhead of normal buffer pool here
    auto write_to_addr =
      reinterpret_cast<u8 *>(buffer_->ToPtr(blob->extents.extent_pid[last_idx])) + last_alloc_sz - remain_free_bytes;
    write_size = std::min(payload.size(), remain_free_bytes);
    std::memcpy(write_to_addr, payload.data(), write_size);
    blob->blob_size += write_size;

    // Only evict necessary pages
    auto evict_size = static_cast<u64>(std::ceil(static_cast<float>(write_size) / PAGE_SIZE));
    Ensure(pid == blob->extents.extent_pid[last_idx] + ExtentList::ExtentSize(last_idx) - evict_size);
    MARK_EXTENT_EVICT(blob, last_idx, pid, evict_size, txn.ToFlushedLargePages(), txn.ToEvictedExtents());
  }

  return write_size;
}

/**
 * @brief If we grow a Blob which has a special block, then we must:
 * - Allocate a new extent to the end of the current extent sequence
 * - Move the content of the special block to that new extent
 * - Copy the payload to that extent as well
 * - Add the special block to the reusable list in free page manager
 */
auto BlobManager::MoveTailExtent(transaction::Transaction &txn, std::span<const u8> payload, BlobState *blob) -> u64 {
  u64 write_size = 0;

  // 1. Allocate a new extent to the end of the extent sequence
  auto last_idx = blob->extents.extent_cnt++;
  auto pid = blob->extents.extent_pid[last_idx] = buffer_->AllocExtent(last_idx, 0);

  // 2. Move the content of special block to the new allocated extent
  auto special_blk_data_sz = blob->blob_size - ExtentList::TotalSizeExtents(last_idx - 1) * PAGE_SIZE;
  Ensure(special_blk_data_sz <= blob->extents.special_blk.page_cnt * PAGE_SIZE);
  auto from_addr = reinterpret_cast<u8 *>(buffer_->ToPtr(blob->extents.special_blk.start_pid));
  if (FLAGS_blob_normal_buffer_pool) {
    buffer_->ChunkOperation(pid, special_blk_data_sz, [&](u64 offset, std::span<u8> page_addr) {
      std::memcpy(page_addr.data(), from_addr + offset, page_addr.size());
    });
  } else {
    std::memcpy(buffer_->ToPtr(pid), from_addr, special_blk_data_sz);
  }

  // 3. Move the content of new payload to the new allocated extent as well
  // TODO(Duy): Add overhead of normal buffer pool here
  write_size = std::min(payload.size(), ExtentList::ExtentSize(last_idx) * PAGE_SIZE - special_blk_data_sz);
  std::memcpy(reinterpret_cast<u8 *>(buffer_->ToPtr(pid)) + special_blk_data_sz, payload.data(), write_size);
  blob->blob_size += write_size;

  // 4. Mark the last extent for Evict()
  MARK_EXTENT_EVICT(blob, last_idx, pid, ExtentList::ExtentSize(last_idx), txn.ToFlushedLargePages(),
                    txn.ToEvictedExtents());

  // 5. Remove all the splitted extents of special block from `extent_loaded` status
  blob->extents.special_blk.SplitToExtents([&](pageid_t pid, [[maybe_unused]] extidx_t) { extent_loaded.remove(pid); });

  // 6. Mark the special block as unused and add the extents to the reusable list
  PREPARE_FREE_SPECIAL_BLK(blob);
  std::memset(&(blob->extents.special_blk), 0, sizeof(TailExtent));
  Ensure(blob->extents.special_blk.in_used == false);

  return write_size;
}

// -------------------------------------------------------------------------------------

/**
 * @brief Allocate new BLOB and mark the new content for async-write in Group Commit
 * For now, only support:
 * - Allocating a whole new blob
 * - Appending content to an existing blob
 *
 * *NOTE*: Do not mark this newly allocated Blob as full load, because if we do it,
 *  then during transaction commit, those db pages will be UnfixShare-ed, causing undefined behavior
 *  Only committed Blobs will be in the `extent_loaded` life cycle
 *
 * @param payload           Payload of this Blob
 * @param prev_blob         The BlobState refers to prev blob, nullptr if this is a new allocation
 * @param likely_grow       Whether it's likely that this new allocated Blob will grow in the future
 *                          If it isn't, then we will allocate a special block (if necessary) to store its tail data
 */
auto BlobManager::AllocateBlob(std::span<const u8> payload, const BlobState *prev_blob, bool likely_grow)
  -> BlobState * {
  // Run-time environment
  blob_handler_storage = {0};
  auto out_blob        = reinterpret_cast<BlobState *>(blob_handler_storage.data());
  active_blob          = out_blob;

  // BLOB async write can only be supported if logging is enabled
  Ensure(FLAGS_wal_enable);

  // This should be run inside an active txn
  auto &current_txn = transaction::TransactionManager::active_txn;
  Ensure(current_txn.IsRunning());

  // If this is a growing operator, load the content of previous blob into memory
  // We only need the content of all previous extents, i.e. don't need them in contiguous memory
  if (prev_blob != nullptr) { LoadBlobContent(prev_blob, prev_blob->blob_size); }

  /**
   * @brief Special block initialization:
   * - Initially, special block should not be eagerly used/allocated
   * - Special block should only be allocated if:
   *  - This is a fresh allocation, i.e. prev_blob == nullptr
   *  - User specifies that the new blob is unlikely to grow, i.e. likely_grow == false
   * - If this is an append allocation (prev_blob != nullptr), it is ridiculous that it won't be appended in the future
   * - If FLAGS_blob_tail_extent is disabled, turn off the special_block feature as well
   */
  out_blob->extents.special_blk.in_used = false;
  if ((prev_blob != nullptr) || (!FLAGS_blob_tail_extent)) { likely_grow = true; }

  // Prepare blob data in the buffer manager
  if (prev_blob == nullptr) {
    FreshBlobAllocation(payload, out_blob, likely_grow, current_txn.ToFlushedLargePages(),
                        current_txn.ToEvictedExtents());
  } else {
    std::memcpy(out_blob, prev_blob, prev_blob->MallocSize());
    u64 write_sz_to_remaining_pages = 0;

    if (prev_blob->extents.special_blk.in_used) {
      write_sz_to_remaining_pages = MoveTailExtent(current_txn, payload, out_blob);
    } else {
      write_sz_to_remaining_pages = WriteNewDataToLastExtent(current_txn, payload, out_blob);
    }

    // If the new payload is written out fully, then there is no need to extend the existing blob
    if (payload.size() > write_sz_to_remaining_pages) {
      // Otherwise, extend prev blob with un-written new data
      auto to_write_payload =
        std::span<const u8>{payload.data() + write_sz_to_remaining_pages, payload.size() - write_sz_to_remaining_pages};
      ExtendExistingBlob(to_write_payload, out_blob, current_txn.ToFlushedLargePages(), current_txn.ToEvictedExtents());
    }
  }

  // Calculate SHA-256 value for the Blob Handler
  BlobState::sha_context.Initialize();
  auto offset = 0UL;
  for (auto &extent : out_blob->extents) { SHA2_CALC_LP(extent); }
  if (offset < out_blob->blob_size) {
    // If we haven't calculated SHA-256 for this BLOB, this means we have the special block
    Ensure(out_blob->extents.special_blk.in_used);
    SHA2_CALC_LP(out_blob->extents.special_blk);
  }
  BlobState::sha_context.Serialize(&out_blob->sha256_intermediate[0]);
  BlobState::sha_context.Final(out_blob->sha2_val);

  return out_blob;
}

/**
 * @brief Remember to remove all references to this BlobState first before calling this func
 *
 * TODO(Duy): Should we somehow X-lock this BlobState before removing it? Probably we need Concurrency Control
 */
void BlobManager::RemoveBlob(const BlobState *blob) {
  // De-allocate the rest one-by-one
  for (size_t idx = 0; idx < blob->extents.NumberOfExtents(); idx++) {
    buffer_->GetFreePageManager()->PrepareFreeTier(blob->extents.extent_pid[idx], idx);
  }
  if (blob->extents.special_blk.in_used) { PREPARE_FREE_SPECIAL_BLK(blob); }
}

void BlobManager::LoadBlob(const BlobState *blob, u64 required_load_size, const BlobCallbackFunc &cb) {
  // Don't read more the the capacity of the Blob
  if (required_load_size > blob->blob_size || required_load_size == 0) { required_load_size = blob->blob_size; }

  LoadBlobContent(blob, required_load_size);
  auto guard = PageAliasGuard(buffer_, *blob, required_load_size);
  cb({guard.GetPtr(), required_load_size});
}

void BlobManager::UnloadAllBlobs() {
  // Unfix all Blobs
  for (const auto &extent_pid : extent_loaded) { buffer_->UnfixShare(extent_pid); }
  extent_loaded.clear();
}

// -------------------------------------------------------------------------------------
auto BlobManager::BlobStateCompareWithString(const void *a, const void *b) -> int {
  auto lhs      = reinterpret_cast<const BlobState *>(a);
  auto rhs      = reinterpret_cast<const BlobLookupKey *>(b);
  auto b_length = rhs->blob.size();

  // 1. Compare Prefix
  int ret = std::memcmp(lhs->blob_prefix, rhs->blob.data(), BlobState::PREFIX_LENGTH);
  if (ret != 0 || b_length < BlobState::PREFIX_LENGTH) { return ret; }

  // 2. Evaluate SHA-256 computed value
  if (std::memcmp(lhs->sha2_val, rhs->sha2_digest, BlobState::SHA256_DIGEST_LENGTH) == 0) { return 0; }

  // 3. Compare Extents of Blob incrementally
  u64 offset = 0;
  for (u8 idx = 0; idx < lhs->extents.extent_cnt; idx++) {
    if (!extent_loaded.contains(lhs->extents.extent_pid[idx])) {
      extent_loaded.add(lhs->extents.extent_pid[idx]);
      buffer_->ReadExtents({LargePage(lhs->extents.extent_pid[idx], ExtentList::ExtentSize(idx))});
    }
    u64 extent_size      = ExtentList::ExtentSize(idx) * PAGE_SIZE;
    auto to_compare_size = std::min(b_length - offset, extent_size);
    // TODO(Duy): Add overhead of normal Buffer Pool here
    ret = std::memcmp(buffer_->ToPtr(lhs->extents.extent_pid[idx]), rhs->blob.data() + offset, to_compare_size);
    if (ret != 0) { return ret; }
    offset += extent_size;
  }

  // 4. All the prefix extents are identical, so we compare the whole content
  if (std::min(lhs->blob_size, b_length) > offset) {
    auto load_size = std::min(lhs->blob_size, b_length);
    LoadBlob(lhs, load_size, [&](std::span<const u8> lhs_payload) {
      ret = std::memcmp(lhs_payload.data() + offset, rhs->blob.data() + offset,
                        std::min(lhs_payload.size(), b_length) - offset);
    });
    if (ret != 0) { return ret; }
  }

  return lhs->blob_size - b_length;
}

auto BlobManager::BlobStateComparison(const void *a, const void *b) -> int {
  auto lhs = reinterpret_cast<const BlobState *>(a);
  auto rhs = reinterpret_cast<const BlobState *>(b);

  // 1. Compare SHA-256 computed value
  if (std::memcmp(lhs->sha2_val, rhs->sha2_val, BlobState::SHA256_DIGEST_LENGTH) == 0) { return 0; }

  // 2. Compare Prefix
  int ret = std::memcmp(lhs->blob_prefix, rhs->blob_prefix, BlobState::PREFIX_LENGTH);
  if (ret != 0) { return ret; }

  // 3. Compare Extents of Blob incrementally
  u64 prefix_size = 0;
  for (u8 idx = 0; idx < std::min(lhs->extents.extent_cnt, rhs->extents.extent_cnt); idx++) {
    prefix_size = ExtentList::TotalSizeExtents(idx);
    LoadBlobContent(lhs, prefix_size);
    LoadBlobContent(rhs, prefix_size);
    // TODO(Duy): Add overhead of normal Buffer Pool here
    ret = std::memcmp(buffer_->ToPtr(lhs->extents.extent_pid[idx]), buffer_->ToPtr(rhs->extents.extent_pid[idx]),
                      ExtentList::ExtentSize(idx) * PAGE_SIZE);
    if (ret != 0) { return ret; }
  }

  // 4. All the prefix extents are identical, so we compare the whole content
  if (std::min(lhs->blob_size, rhs->blob_size) > prefix_size) {
    auto load_size = std::min(lhs->blob_size, rhs->blob_size);
    LoadBlob(lhs, load_size, [&](std::span<const u8> lhs_payload) {
      LoadBlob(rhs, load_size, [&](std::span<const u8> rhs_payload) {
        ret = std::memcmp(lhs_payload.data() + prefix_size, rhs_payload.data() + prefix_size,
                          std::min(lhs_payload.size(), rhs_payload.size()) - prefix_size);
      });
    });
    if (ret != 0) { return ret; }
  }

  // 5. Prefix of Two Blobs are the same, return whatever is longer
  return lhs->blob_size - rhs->blob_size;
}

}  // namespace leanstore::storage::blob
