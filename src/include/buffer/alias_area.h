#pragma once

#include "common/typedefs.h"
#include "storage/page.h"

#include <atomic>
#include <vector>

namespace leanstore::buffer {

class AliasingArea {
 public:
  AliasingArea(u64 alias_block_size, u64 max_buffer_size, storage::Page *alias_ptr);
  ~AliasingArea() = default;
  auto RequestAliasingArea(u64 requested_size) -> storage::Page *;
  void ReleaseAliasingArea();
  auto ToggleShalasLocks(bool set_op, u64 &block_start, u64 block_end) -> bool;

 private:
  static constexpr u8 NO_BLOCKS_PER_LOCK = sizeof(u64) * CHAR_BIT;

  FRIEND_TEST(TestBufferManager, SharedAliasingLock);
  FRIEND_TEST(TestBufferManager, ConcurrentRequestAliasingArea);

  /* Configuration */
  const u64 block_size_;   /* Size of a local aliasing area */
  const u64 block_pg_cnt_; /* Size of a local aliasing area in PAGE_SIZE */
  const u64 no_blocks_;    /* Number of blocks in shared aliasing area, every block is as big as local_alias_size_ */
  const u64 no_locks_;     /* Number of locks of `range_locks_`, each atomic<u64> manages 64 locks */

  /* Run-time */
  storage::Page *aliasing_area_;              /* Start pointer of all aliasing areas */
  storage::Page *shared_alias_area_;          /* Start pointer of the shared aliasing area */
  std::vector<u64> local_area_ptr_;           /* Current Ptr in Worker-local aliasing area */
  std::atomic<u64> shared_area_ptr_;          /* Current block ptr in shared aliasing area */
  std::vector<std::atomic<u64>> range_locks_; /* Range lock with bitmap, each bit corresponds to a block */
  std::vector<std::vector<std::pair<u64, u64>>> acquired_locks_; /* To UNDO the lock after a successful lock */
};

}  // namespace leanstore::buffer