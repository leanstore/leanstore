#pragma once

#include "common/typedefs.h"

#include <atomic>
#include <bitset>

namespace leanstore::buffer {

enum class PageFlagIdx : u8 { IS_EXTENT = 0, EXTENT_TO_WRITE = 1 };

/**
 * @brief Run-time context of all DB pages
 */
struct BufferFrame {
  wid_t last_writer;                // the id of last worker who modifies this page since its last flush
  std::atomic<bool> prevent_evict;  // whether BufferManager::Evict() is prevented on this page
  /**
   * @brief helps BufferManager::Evict() determine the correct evict size for an extent
   * for normal page, its value should be 1, i.e. evict only 1 page
   */
  u64 evict_pg_count;

  /**
   * @brief Run-time flags of this page
   *
   * The meaning of those bits are mentioned in PageFlagIdx
   * - IS_EXTENT: Whether this page is part of an extent
   * - EXTENT_TO_WRITE: Whether this page should be written or evicted during BufferManager::Evict()
   */
  std::bitset<2> rt_flags;

  void Reset() {
    last_writer    = 0;
    rt_flags       = 0;
    evict_pg_count = 1;
    prevent_evict  = false;
  }

  void SetFlag(PageFlagIdx flag) { rt_flags.set(static_cast<std::underlying_type<PageFlagIdx>::type>(flag)); }

  auto IsExtent() -> bool {
    return rt_flags.test(static_cast<std::underlying_type<PageFlagIdx>::type>(PageFlagIdx::IS_EXTENT));
  }

  auto ToWriteExtent() -> bool {
    return rt_flags.test(static_cast<std::underlying_type<PageFlagIdx>::type>(PageFlagIdx::EXTENT_TO_WRITE));
  }
};

}  // namespace leanstore::buffer