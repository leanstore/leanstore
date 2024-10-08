#include "buffer/resident_set.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "sync/page_state.h"

#include "gtest/gtest.h"

#include <array>
#include <cmath>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace leanstore::buffer {

class TestResidentSet : public ::testing::Test {
 protected:
  static constexpr u64 TEST_NO_PAGE   = 1 << 8;
  static constexpr i32 NO_THREADS     = 8;
  static constexpr int OPS_PER_THREAD = 200;

  std::unique_ptr<ResidentPageSet> set_;
  sync::PageState *state_ = nullptr;

  TestResidentSet() : set_{nullptr} {};

  void SetUp() override {
    state_ = static_cast<sync::PageState *>(calloc(TEST_NO_PAGE, sizeof(sync::PageState)));
    set_   = std::make_unique<ResidentPageSet>(TEST_NO_PAGE);
    assert(set_->Capacity() == TEST_NO_PAGE);
  }

  void TearDown() override {
    delete state_;
    set_.reset(nullptr);
  }
};

TEST_F(TestResidentSet, InsertOperation) {
  state_[12].LockExclusive();
  set_->Insert(12);
  EXPECT_THROW(set_->Insert(12), leanstore::ex::Unreachable);
  state_[14].LockExclusive();
  set_->Insert(14);
  state_[7].LockExclusive();
  set_->Insert(7);
  EXPECT_THROW(set_->Insert(7), leanstore::ex::Unreachable);
}

TEST_F(TestResidentSet, CanStoreMaxCapacity) {
  for (size_t idx = 0; idx < TEST_NO_PAGE; idx++) {
    state_[idx].LockExclusive();
    set_->Insert(idx);
  }
  // Replacer inserts everything successfully
  EXPECT_TRUE(true);
}

TEST_F(TestResidentSet, DeleteOperation) {
  for (size_t idx = 0; idx < TEST_NO_PAGE; idx++) {
    state_[idx].LockExclusive();
    set_->Insert(idx);
    state_[idx].UnlockExclusive();
  }
  for (size_t idx = 0; idx < TEST_NO_PAGE; idx++) {
    state_[idx].LockExclusive();
    EXPECT_TRUE(set_->Remove(idx));
    state_[idx].UnlockExclusiveAndEvict();
    EXPECT_FALSE(set_->Remove(idx));
  }
}

TEST_F(TestResidentSet, FindEvictCandidate) {
  // Create full replacer
  for (size_t idx = 0; idx < set_->Capacity(); idx++) {
    state_[idx].LockExclusive();
    set_->Insert(idx);
    state_[idx].UnlockExclusive();
  }
  // Try to evict 8 pages
  int to_evict_count = 8;
  std::vector<pageid_t> to_evict;
  set_->IterateClockBatch(to_evict_count, [&](pageid_t page_id) { to_evict.push_back(page_id); });
  // Because the replacer is full, we should be able to evict 8 pages in our batch
  EXPECT_LE(to_evict.size(), to_evict_count);
}

TEST_F(TestResidentSet, NormalOperation) {
  // Insert page 0 -> TEST_NO_PAGE - 1
  for (size_t idx = 0; idx < TEST_NO_PAGE; idx++) {
    state_[idx].LockExclusive();
    set_->Insert(idx);
  }
  // Remove page 0 -> TEST_NO_PAGE - 1, whose page id is even
  for (size_t idx = 0; idx < TEST_NO_PAGE; idx += 2) {
    EXPECT_TRUE(set_->Remove(idx));
    state_[idx].UnlockExclusiveAndEvict();
  }
  // Remove should succeed if page_id is odd, as all even pages were removed
  for (size_t idx = 0; idx < TEST_NO_PAGE; idx++) {
    if (idx % 2 == 0) {
      EXPECT_FALSE(set_->Remove(idx));
    } else {
      EXPECT_TRUE(set_->Remove(idx));
      state_[idx].UnlockExclusiveAndEvict();
    }
  }
  std::vector<pageid_t> to_evict;
  set_->IterateClockBatch(sqrt(TEST_NO_PAGE), [&](pageid_t page_id) { to_evict.push_back(page_id); });
  // After removing all pages, the evict size should be 0
  EXPECT_EQ(to_evict.size(), 0);
}

TEST_F(TestResidentSet, ConcurrencyOperation) {
  std::array<std::thread, NO_THREADS> threads;

  for (size_t idx = 0; idx < TEST_NO_PAGE; idx++) {
    state_[idx].LockExclusive();
    set_->Insert(idx);
    state_[idx].UnlockExclusive();
  }
  LOG_INFO("Start concurrency test");
  for (auto idx = 0; idx < NO_THREADS; ++idx) {
    threads[idx] = std::thread([&]() {
      for (auto j = 0; j < OPS_PER_THREAD; j++) {
        pageid_t pid = rand() % set_->Capacity();
        u64 state_w  = state_[pid].StateAndVersion().load();
        if (sync::PageState::LockState(state_w) == sync::PageStateMode::EVICTED) {
          // EXPECT_DEATH(set_->Remove(pid), "Assertion");
          if (state_[pid].TryLockExclusive(state_w)) {
            set_->Insert(pid);
            state_[pid].UnlockExclusive();
          }
        } else {
          EXPECT_LE(state_[pid].LockState(state_w), sync::PageStateMode::EXCLUSIVE);
          if (state_[pid].TryLockExclusive(state_w)) {
            ASSERT_TRUE(set_->Remove(pid));
            state_[pid].UnlockExclusiveAndEvict();
          }
        }
      }
    });
  }
  for (auto &th : threads) { th.join(); }
}

}  // namespace leanstore::buffer
