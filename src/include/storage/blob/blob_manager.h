#pragma once

#include "buffer/buffer_manager.h"
#include "storage/blob/blob_state.h"

#include "gtest/gtest_prod.h"
#include "roaring/roaring.hh"
#include "share_headers/db_types.h"

#include <functional>
#include <memory>
#include <span>
#include <unordered_set>

namespace leanstore::storage::blob {

using BlobCallbackFunc = std::function<void(std::span<const u8>)>;

class BlobManager {
 public:
  static thread_local BlobState *active_blob;
  static thread_local roaring::Roaring64Map extent_loaded;
  static thread_local std::array<u8, BlobState::MallocSize(ExtentList::EXTENT_CNT_MASK)> blob_handler_storage;

  explicit BlobManager(buffer::BufferManager *buffer_manager);
  ~BlobManager() = default;

  // Blob allocate/deallocate
  auto AllocateBlob(std::span<const u8> payload, const BlobState *prev_blob, bool likely_grow = true) -> BlobState *;
  void RemoveBlob(BlobState *blob);

  // Blob Load/Unload utilities
  void LoadBlob(const BlobState *blob, u64 required_load_size, const BlobCallbackFunc &cb);
  void UnloadAllBlobs();

  // Comparator utilities
  auto BlobStateCompareWithString(const void *a, const void *b) -> int;
  auto BlobStateComparison(const void *a, const void *b) -> int;

 private:
  void LoadBlobContent(const BlobState *blob, u64 required_load_size);

  // Move data utilities
  auto WriteNewDataToLastExtent(transaction::Transaction &txn, std::span<const u8> payload, BlobState *blob) -> u64;
  auto MoveTailExtent(transaction::Transaction &txn, std::span<const u8> payload, BlobState *blob) -> u64;

  // Allocation utilities
  void AllocateRemainContent(BlobState *blob, std::span<const u8> payload, u64 &offset, extidx_t tier,
                             bool is_tail_extent);
  void FreshBlobAllocation(std::span<const u8> payload, BlobState *out_blob, bool likely_grow,
                           LargePageList &out_to_write_lps, std::vector<pageid_t> &out_to_evict_ets);
  void ExtendExistingBlob(std::span<const u8> payload, BlobState *out_blob, LargePageList &out_to_write_lps,
                          std::vector<pageid_t> &out_to_evict_ets);

  buffer::BufferManager *buffer_;
};

}  // namespace leanstore::storage::blob