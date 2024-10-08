#include "buffer/buffer_manager.h"
#include "common/typedefs.h"
#include "sync/page_state.h"
#include "test/base_test.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <memory>

using ExtentList = leanstore::storage::ExtentList;

namespace leanstore::buffer {

static constexpr int NO_LOCK_OPS = 2000;
static constexpr int NO_THREADS  = 10;

class TestBufferManager : public BaseTest {
 protected:
  void SetUp() override { BaseTest::SetupTestFile(); }

  void TearDown() override {
    BaseTest::TearDown();
    recovery::LogManager::global_min_gsn_flushed.store(0);
  }
};

TEST_F(TestBufferManager, BasicTest) {
  size_t in_memory_cap = std::round(PHYSICAL_CAP * 0.9) - 1;

  EXPECT_EQ(buffer_->physical_used_cnt_, 1);
  // Initially, all pages should be in UNLOCKED mode, i.e. not yet initialized
  for (u64 idx = 1; idx < N_PAGES; idx++) {
    auto &ps = buffer_->GetPageState(idx);
    EXPECT_EQ(ps.LockState(), sync::PageState::UNLOCKED);
    EXPECT_EQ(ps.Version(), 0);
  }

  // Allocate in_memory_cap pages, modify their (+ page 0's) content, and validate their mode
  for (u64 idx = 0; idx < in_memory_cap; idx++) {
    auto page = (idx > 0) ? (buffer_->AllocPage()) : buffer_->FixExclusive(0);
    ModifyPageContent(page);
    auto &ps = buffer_->GetPageState(idx);
    EXPECT_EQ(ps.LockState(), sync::PageState::EXCLUSIVE);
    EXPECT_EQ(ps.Version(), (idx == 0) ? 1 : 0);
  }
  EXPECT_EQ(buffer_->physical_used_cnt_, in_memory_cap);
  // Unfix them
  LOG_DEBUG("Unfix all pages");
  for (u64 pid = 0; pid < in_memory_cap; pid++) {
    buffer_->UnfixExclusive(pid);
    auto &ps = buffer_->GetPageState(pid);
    EXPECT_EQ(ps.LockState(), sync::PageState::UNLOCKED);
    EXPECT_EQ(ps.Version(), (pid == 0) ? 2 : 1);
  }
  // The buffer pool should be full now (we allocate its full in-memory capacity)
  //  hence, by allocating 1 extra page, it forces the buffer pool to evict EVICT_SIZE pages
  LOG_DEBUG("Start evict all pages");
  // Simulate that all logs has been flushed
  recovery::LogManager::global_min_gsn_flushed.store(9999999);
  buffer_->AllocPage();
  std::vector<pageid_t> evicted_pages;

  for (u64 pid = 0; pid < in_memory_cap; pid++) {
    auto &ps = buffer_->GetPageState(pid);
    // page should be in either MARKED or EVICTED state
    if (ps.LockState() == sync::PageState::EVICTED) {
      evicted_pages.push_back(pid);
    } else {
      EXPECT_EQ(ps.LockState(), sync::PageState::MARKED);
    }
  }
  EXPECT_GE(evicted_pages.size(), EVICT_SIZE);
  EXPECT_EQ(buffer_->physical_used_cnt_, in_memory_cap - evicted_pages.size() + 1);
  LOG_DEBUG("Read back evicted pages");
  for (auto &pid : evicted_pages) {
    auto &ps = buffer_->GetPageState(pid);
    EXPECT_EQ(ps.LockState(), sync::PageState::EVICTED);
    auto page  = buffer_->FixShare(pid);
    u8 *buffer = reinterpret_cast<u8 *>(page);
    EXPECT_EQ(buffer_->PageIsDirty(pid), false);
    for (size_t idx = sizeof(storage::PageHeader); idx < PAGE_SIZE; ++idx) { EXPECT_EQ(buffer[idx], 111); }
  }
}

TEST_F(TestBufferManager, BasicTestWithExtent) {
  InitRandTransaction();
  auto &txn         = transaction::TransactionManager::active_txn;
  auto extent_count = 6U;

  // Initially, all pages should be in UNLOCKED mode, i.e. not yet initialized
  for (u64 idx = 1; idx < N_PAGES; idx++) {
    auto &ps = buffer_->GetPageState(idx);
    EXPECT_EQ(ps.LockState(), sync::PageState::UNLOCKED);
    EXPECT_EQ(ps.Version(), 0);
  }
  EXPECT_EQ(buffer_->physical_used_cnt_, 1);

  // Allocate `extent_count` contiguous extents and validate their state
  std::vector<pageid_t> start_pids;
  for (size_t idx = 0; idx < extent_count; idx++) { start_pids.emplace_back(buffer_->AllocExtent(idx)); }
  auto total_ext_size = ExtentList::TotalSizeExtents(extent_count - 1);

  EXPECT_EQ(start_pids[0], N_PAGES + 1);
  EXPECT_EQ(buffer_->physical_used_cnt_, total_ext_size + 1);
  for (auto idx = 0U; idx < extent_count; idx++) {
    EXPECT_EQ(start_pids[idx] - start_pids[0], (idx == 0) ? 0 : N_PAGES * idx);
    CHECK_EXTENT_PAGE_STATE(sync::PageState::EXCLUSIVE, start_pids[idx], ExtentList::ExtentSize(idx));
    for (auto pid = start_pids[idx]; pid < start_pids[idx] + ExtentList::ExtentSize(idx); pid++) {
      auto &ps = buffer_->GetPageState(pid);
      EXPECT_EQ(ps.Version(), 0);
    }
  }

  // Modify the in-memory content of these extents
  LOG_DEBUG("Modify these extents");
  for (auto idx = 0U; idx < extent_count; idx++) {
    auto ext_size = ExtentList::ExtentSize(idx);
    std::memset(reinterpret_cast<u8 *>(buffer_->ToPtr(start_pids[idx])), 111, ext_size * PAGE_SIZE);
    buffer_->PrepareExtentEviction(start_pids[idx]);
    CHECK_EXTENT_PAGE_STATE(sync::PageState::UNLOCKED, start_pids[idx], ext_size);

    // Check frame info
    EXPECT_EQ(buffer_->BufferFrame(start_pids[idx]).prevent_evict.load(), true);
    EXPECT_EQ(buffer_->GetPageState(start_pids[idx]).Version(), 1);
  }

  // Flush & then Evict all these extents
  LOG_DEBUG("Mark this BLOB as EVICTED");
  auto ret = 0;
  for (auto idx = 0U; idx < extent_count; idx++) {
    ret += pwrite(buffer_->blockfd_, buffer_->ToPtr(start_pids[idx]), ExtentList::ExtentSize(idx) * PAGE_SIZE,
                  start_pids[idx] * PAGE_SIZE);
  }
  ASSERT_EQ(ret, static_cast<int>(total_ext_size * PAGE_SIZE));
  for (auto idx = 0U; idx < extent_count; idx++) { buffer_->EvictExtent(start_pids[idx]); }

  // Evict the whole buffer pool
  LOG_DEBUG("Evict the whole buffer pool");
  while (buffer_->physical_used_cnt_ > 1) { buffer_->Evict(); }
  EXPECT_EQ(buffer_->physical_used_cnt_, 1);
  EXPECT_TRUE((buffer_->GetPageState(0).LockState() == sync::PageState::MARKED) ||
              (buffer_->GetPageState(0).LockState() == sync::PageState::UNLOCKED));

  // Read back all the evicted extents
  LOG_DEBUG("Read back evicted extents using large page API");
  storage::LargePageList extents;
  for (auto idx = 0U; idx < extent_count; idx++) { extents.emplace_back(start_pids[idx], ExtentList::ExtentSize(idx)); }
  buffer_->ReadExtents(extents);
  EXPECT_EQ(buffer_->physical_used_cnt_, total_ext_size + 1);

  // Call read twice to validate value of physical_used_cnt_
  buffer_->ReadExtents(extents);
  EXPECT_EQ(buffer_->physical_used_cnt_, total_ext_size + 1);
  for (auto idx = 0U; idx < extent_count; idx++) {
    for (u64 pid = start_pids[idx]; pid < start_pids[idx] + ExtentList::ExtentSize(idx); pid++) {
      auto &ps = buffer_->GetPageState(pid);
      EXPECT_EQ(ps.LockState(),
                (pid == start_pids[idx]) ? 2 : sync::PageState::UNLOCKED);  // Read twice -> Share cnt should be 2
      u8 *buffer = reinterpret_cast<u8 *>(buffer_->ToPtr(pid));
      for (size_t idx = 0; idx < PAGE_SIZE; ++idx) { ASSERT_EQ(buffer[idx], 111); }
    }
  }

  // Now try to free all the Extents
  ASSERT_TRUE(txn.ToFreeExtents().empty());
  for (auto idx = 0U; idx < extent_count; idx++) {
    // Unlock Shared twice, because we read these extents twice in this test
    buffer_->GetPageState(start_pids[idx]).UnlockShared();
    buffer_->GetPageState(start_pids[idx]).UnlockShared();
    txn.ToFreeExtents().emplace_back(start_pids[idx], idx);
  }
}

TEST_F(TestBufferManager, AllocFullCapacity) {
  // Assume that all logs were flushed
  recovery::LogManager::global_min_gsn_flushed.store(9999999);

  // Page 0 is already allocated, hence we only allocate N_PAGES - 1 pages
  ASSERT_EQ(buffer_->alloc_cnt_, 1);
  for (size_t idx = 0; idx < N_PAGES - 1; idx++) {
    auto page = buffer_->AllocPage();
    buffer_->UnfixExclusive(buffer_->ToPID(page));
  }
  ASSERT_EQ(buffer_->alloc_cnt_, N_PAGES);
}

TEST_F(TestBufferManager, SharedAliasingLock) {
  EXPECT_EQ(buffer_->AliasArea()->no_blocks_, N_PAGES / EXTRA_NO_PG);
  EXPECT_EQ(buffer_->AliasArea()->no_locks_, (N_PAGES / EXTRA_NO_PG) / BufferManager::NO_BLOCKS_PER_LOCK);

  // Initial acquire lock
  u64 start_pos = 64;
  EXPECT_TRUE(buffer_->AliasArea()->ToggleShalasLocks(true, start_pos, 120));
  EXPECT_EQ(start_pos, 120);
  EXPECT_EQ(buffer_->AliasArea()->range_locks_[0].load(), 0);
  EXPECT_EQ(buffer_->AliasArea()->range_locks_[1].load(), (1UL << (120 - 64)) - 1);

  // Conflict lock acquisation should fail
  u64 new_pos = 16;
  start_pos   = new_pos;
  EXPECT_FALSE(buffer_->AliasArea()->ToggleShalasLocks(true, start_pos, 80));
  EXPECT_EQ(start_pos, 64);  // Should successfully acquire partial lock
  EXPECT_EQ(buffer_->AliasArea()->range_locks_[0].load(), ULONG_MAX - ((1 << 16) - 1));
  EXPECT_EQ(buffer_->AliasArea()->range_locks_[1].load(), (1UL << (120 - 64)) - 1);

  // Unlock should always succeed
  EXPECT_TRUE(buffer_->AliasArea()->ToggleShalasLocks(false, new_pos, start_pos));
  EXPECT_EQ(new_pos, start_pos);
  EXPECT_EQ(buffer_->AliasArea()->range_locks_[0].load(), 0);
  EXPECT_EQ(buffer_->AliasArea()->range_locks_[1].load(), (1UL << (120 - 64)) - 1);
}

TEST_F(TestBufferManager, ConcurrentRequestAliasingArea) {
  std::thread threads[NO_THREADS];

  for (int t_id = 0; t_id < NO_THREADS; t_id++) {
    threads[t_id] = std::thread([&, t_id]() {
      worker_thread_id = t_id;

      for (auto idx = 0; idx < NO_LOCK_OPS; idx++) {
        // Request blob size which is bigger than the local aliasing area
        auto blob_pg_cnt = (EXTRA_NO_PG + 1) + rand() % (N_PAGES - EXTRA_NO_PG - 1);

        // Request op
        auto alias_ptr = buffer_->AliasArea()->RequestAliasingArea(blob_pg_cnt * PAGE_SIZE);
        auto block_cnt = std::ceil(static_cast<float>(blob_pg_cnt) / EXTRA_NO_PG);
        EXPECT_TRUE((alias_ptr - buffer_->AliasArea()->shared_alias_area_) % EXTRA_NO_PG == 0);
        auto block_idx = (alias_ptr - buffer_->AliasArea()->shared_alias_area_) / EXTRA_NO_PG;

        // Check that all bits should be set
        ASSERT_EQ(buffer_->AliasArea()->acquired_locks_[t_id].size(), 1);
        ASSERT_THAT(buffer_->AliasArea()->acquired_locks_[t_id][0], testing::Pair(block_idx, block_cnt));
        for (auto bl_i = block_idx; bl_i < block_idx + block_cnt; bl_i++) {
          auto bl        = bl_i / buffer::AliasingArea::NO_BLOCKS_PER_LOCK;
          auto bl_offset = bl_i % buffer::AliasingArea::NO_BLOCKS_PER_LOCK;
          ASSERT_GT(buffer_->AliasArea()->range_locks_[bl].load() & (1UL << bl_offset), 0);
        }

        // Release the locks
        buffer_->AliasArea()->ReleaseAliasingArea();

        // Special check when there is only 1 running thread
        if (NO_THREADS == 1) {
          for (u64 x = 0; x < buffer_->AliasArea()->no_locks_; x++) {
            ASSERT_EQ(buffer_->AliasArea()->range_locks_[x].load(), 0);
          }
        }
      }
    });
  }

  for (auto &thread : threads) { thread.join(); }
}

}  // namespace leanstore::buffer

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_bm_aio_qd    = 8;
  FLAGS_worker_count = 12;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
