#pragma once

#include "common/typedefs.h"

#include <atomic>
#include <bitset>

namespace leanstore::buffer {

/**
 * @brief Run-time context of all DB pages
 */
struct BufferFrame {
  wid_t last_writer;                // the id of last worker who modifies this page since its last flush
  u64 last_written_gsn;             // the last gsn that this extent/page was written to storage
  u64 extent_size;                  // number of pages for this extent, only used when enable_extent_tier_==false
  std::atomic<bool> prevent_evict;  // whether BufferManager::Evict() is prevented on this page

  void Init() {
    last_writer      = 0;
    last_written_gsn = 0;
    extent_size      = 1;
    prevent_evict    = false;
  }
};

}  // namespace leanstore::buffer