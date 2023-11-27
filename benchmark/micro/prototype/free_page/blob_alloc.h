#pragma once

#include "impl.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <iterator>
#include <list>
#include <memory>
#include <span>

using BlobRepStorage = uint8_t[1234];

struct BlobRep {
  static constexpr int MAX_TIER = 16;  // 0 -> MAX_TIER (MAX_TIER + 1 first extents),
                                       // the MAX_TIER+1 ext goes on have same size as ext MAX_TIER +1
  static constexpr size_t MAX_EXT_SIZE = 1 << MAX_TIER;

  int extent_cnt;
  size_t extent_pid[];

  auto MallocSize() -> size_t { return sizeof(extent_cnt) + extent_cnt * sizeof(size_t); }
};

/**
 * @brief Basically `requested_size` and `append_size` aren't necessary to be perfect
 * i.e. new allocated extents fit the requested size perfectly
 * however, for simplicity, we assume that all allocated extents are filled 100%,
 * that means, `requested_size` is only the lower bound of the real allocated size in this dump simulator
 * and the real size should be the total size of all allocated extents
 */
struct BufferManager {
  FreePageManager *man;
  std::list<BlobRepStorage> blob_list;

  BufferManager(FreePageManager *manager) : man(manager) {}

  static auto TierIndex(int extent_cnt) { return std::min(extent_cnt, BlobRep::MAX_TIER); }

  auto BlobCount() -> size_t { return std::distance(blob_list.begin(), blob_list.end()); }

  auto AllocBlob(int requested_size) -> bool {
    blob_list.emplace_front();

    bool success = AppendBlob(0, requested_size);
    if (!success) { blob_list.pop_front(); }

    return success;
  }

  auto AppendBlob(int index, int append_size) -> bool {
    auto blob_storage = std::next(blob_list.begin(), index);
    auto blob         = reinterpret_cast<BlobRep *>(*blob_storage);

    bool success       = false;
    int old_extent_cnt = blob->extent_cnt;
    while (append_size > 0) {
      size_t pid;
      success = man->AllocTierPage(TierIndex(blob->extent_cnt), pid);
      if (!success) { break; }
      blob->extent_cnt++;
      blob->extent_pid[blob->extent_cnt - 1] = pid;
      append_size -= TIER_SIZE(TierIndex(blob->extent_cnt - 1));
    }
    assert(blob->MallocSize() < 1234);
    if (!success) {
      for (auto idx = old_extent_cnt; idx < blob->extent_cnt; idx++) {
        man->DeallocTierPage(blob->extent_pid[idx], TierIndex(idx));
      }
      blob->extent_cnt = old_extent_cnt;
    }

    return success;
  }

  void RemoveBlob(int index) {
    auto blob_storage = std::next(blob_list.begin(), index);
    auto blob         = reinterpret_cast<BlobRep *>(*blob_storage);
    for (auto idx = 0; idx < blob->extent_cnt; idx++) { man->DeallocTierPage(blob->extent_pid[idx], TierIndex(idx)); }
    blob_list.erase(blob_storage);
  }
};