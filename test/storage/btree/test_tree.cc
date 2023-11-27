#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "test/base_test.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"

#include <cstring>
#include <unordered_map>

namespace leanstore::storage {

static constexpr u32 NO_RECORDS = 10000;
static constexpr u32 NO_THREADS = 10;

class TestBTree : public BaseTest {
 protected:
  std::unique_ptr<BTree> tree_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<BTree>(buffer_.get());
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }

  template <typename value_t>
  void PrepareData(std::vector<std::pair<int, value_t>> &data, bool get_permutation = false, bool prefill_tree = true) {
    for (size_t idx = 0; idx < NO_RECORDS; idx++) {
      data.emplace_back(static_cast<int>(__builtin_bswap32(idx + 1)), static_cast<value_t>(idx * 100));
    }
    if (get_permutation) {
      for (size_t idx = 0; idx < rand() % NO_RECORDS + NO_RECORDS / 2; idx++) {
        std::next_permutation(data.begin(), data.end());
      }
    }
    Ensure(!tree_->IsNotEmpty());
    if (prefill_tree) {
      for (auto &pair : data) {
        std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
        std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(value_t)};
        tree_->Insert(key, payload);
      }
      Ensure(tree_->IsNotEmpty());
    }
  }
};

TEST_F(TestBTree, InsertAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, true);

  for (auto &pair : data) {
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(__uint128_t)};

    auto found = tree_->LookUp(key, [&payload](std::span<u8> data) {
      EXPECT_EQ(payload.size(), data.size());
      for (size_t idx = 0; idx < data.size(); idx++) { EXPECT_EQ(payload[idx], data[idx]); }
    });
    ASSERT_TRUE(found);
  }
}

TEST_F(TestBTree, RemoveAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, false);
  std::array<bool, NO_RECORDS + 1> removed_f = {false};

  ASSERT_TRUE(tree_->IsNotEmpty());

  // Remove random
  for (auto idx = 0; idx < static_cast<int>(NO_RECORDS); idx++) {
    int int_key     = rand() % NO_RECORDS + 1;
    int ordered_key = __builtin_bswap32(int_key);
    std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};

    if (!removed_f[int_key]) {
      auto success = tree_->Remove(key);
      ASSERT_TRUE(success);
      removed_f[int_key] = true;
    } else {
      auto success = tree_->Remove(key);
      ASSERT_FALSE(success);
    }

    auto found = tree_->LookUp(key, PayloadFunc());
    ASSERT_FALSE(found);
  }

  // Now remove all
  for (auto idx = 1; idx <= static_cast<int>(NO_RECORDS); idx++) {
    if (!removed_f[idx]) {
      int ordered_key = __builtin_bswap32(idx);
      std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};
      auto success = tree_->Remove(key);
      ASSERT_TRUE(success);
      auto found = tree_->LookUp(key, PayloadFunc());
      ASSERT_FALSE(found);
    }
  }

  ASSERT_FALSE(tree_->IsNotEmpty());
}

TEST_F(TestBTree, UpdateAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, false);

  ASSERT_TRUE(tree_->IsNotEmpty());

  // Remove random
  for (auto idx = 0; idx < static_cast<int>(NO_RECORDS); idx++) {
    int int_key     = rand() % NO_RECORDS + 1;
    int ordered_key = __builtin_bswap32(int_key);
    std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&idx), sizeof(int)};

    auto success = tree_->Update(key, payload);
    ASSERT_TRUE(success);

    auto found = tree_->LookUp(key, [&payload](std::span<u8> found_payload) {
      EXPECT_EQ(payload.size(), found_payload.size());
      for (size_t idx = 0; idx < found_payload.size(); idx++) { EXPECT_EQ(payload[idx], found_payload[idx]); }
    });
    ASSERT_TRUE(found);
  }

  ASSERT_EQ(tree_->CountEntries(), NO_RECORDS);
}

TEST_F(TestBTree, TreeScan) {
  std::vector<std::pair<int, int>> data;
  PrepareData<int>(data, false);
  std::unordered_map<int, int> scan_result;

  auto read_cb = [&scan_result](std::span<u8> key, std::span<u8> payload) -> bool {
    scan_result[LoadUnaligned<int>(key.data())] = LoadUnaligned<int>(payload.data());
    return true;
  };

  // Scan Asc
  int start_key = 0;
  std::span key{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  tree_->ScanAscending(key, read_cb);
  EXPECT_EQ(scan_result.size(), data.size());
  for (auto &pair : data) {
    ASSERT_TRUE(scan_result.find(pair.first) != scan_result.end());
    ASSERT_TRUE(scan_result[pair.first] == pair.second);
  }

  // Scan Desc
  start_key = NO_RECORDS + 1;
  key       = std::span<u8>{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  scan_result.clear();
  tree_->ScanDescending(key, read_cb);
  for (auto &pair : data) {
    EXPECT_TRUE(scan_result.find(pair.first) != scan_result.end());
    EXPECT_TRUE(scan_result[pair.first] == pair.second);
  }
  EXPECT_EQ(scan_result.size(), data.size());
}

TEST_F(TestBTree, ConcurrentInsertAndSearch) {
  std::vector<std::pair<int, int>> data;
  std::array<std::atomic<bool>, NO_RECORDS + 1> inserted_f;
  std::atomic<size_t> insert_idx = 0;
  std::thread threads[NO_THREADS];

  PrepareData<int>(data, true, false);
  for (u32 t_id = 0; t_id < NO_THREADS; t_id++) {
    threads[t_id] = std::thread([&, t_id]() {
      InitRandTransaction(t_id);

      // it would take like a year to complete the tests for NO_RECORDS operations
      //  thus, we only run NO_RECORDS / 10 ops
      for (size_t idx = 0; idx < NO_RECORDS / 10; idx++) {
        int r = rand() % 2;
        if (r == 0 && insert_idx < NO_RECORDS) {
          // Insert
          auto index_pos = insert_idx++;
          if (index_pos < data.size()) {
            std::span key{reinterpret_cast<u8 *>(&data[index_pos].first), sizeof(int)};
            std::span payload{reinterpret_cast<u8 *>(&data[index_pos].second), sizeof(int)};
            tree_->Insert(key, payload);
            inserted_f[index_pos] = true;
          }
        } else if (insert_idx.load() > 0) {
          // Search
          auto search_pos = rand() % insert_idx.load();
          while (!inserted_f[search_pos].load()) { search_pos = rand() % insert_idx.load(); }
          std::span key{reinterpret_cast<u8 *>(&data[search_pos].first), sizeof(int)};

          int found_data;
          auto found = tree_->LookUp(
            key, [&found_data](std::span<u8> data) { std::memcpy(&found_data, data.data(), data.size()); });
          EXPECT_EQ(found_data, data[search_pos].second);
          ASSERT_TRUE(found);
        }
      }
    });
  }

  for (auto &thread : threads) { thread.join(); }
}

}  // namespace leanstore::storage

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_bm_aio_qd    = 8;
  FLAGS_worker_count = 12;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
