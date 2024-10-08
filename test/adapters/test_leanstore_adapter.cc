#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/utils/test_utils.h"
#include "leanstore/leanstore.h"
#include "test/base_test.h"

#include "gtest/gtest.h"
#include "share_headers/db_types.h"

namespace leanstore {

static constexpr int NO_RECORDS = 100;

class TestLeanStoreAdapter : public ::testing::Test {
 protected:
  std::unique_ptr<LeanStore> db_;

  void SetUp() override { db_ = std::make_unique<LeanStore>(); }

  void TearDown() override { db_.reset(); }
};

TEST_F(TestLeanStoreAdapter, BasicTest) {
  auto adapter = std::make_unique<LeanStoreAdapter<benchmark::RelationTest>>(*db_);

  db_->worker_pool.ScheduleSyncJob(0, [&]() {
    db_->StartTransaction();
    for (int idx = 0; idx < NO_RECORDS; idx++) {
      benchmark::RelationTest::Key key{idx};
      benchmark::RelationTest data{idx * 5};
      adapter->Insert(key, data);
    }
    EXPECT_EQ(adapter->Count(), NO_RECORDS);
    db_->CommitTransaction();
  });
  db_->worker_pool.ScheduleSyncJob(1, [&]() {
    db_->StartTransaction();
    for (int idx = 0; idx < NO_RECORDS; idx++) {
      benchmark::RelationTest::Key key{idx};
      adapter->LookUp(key, [&idx](const benchmark::RelationTest &record) {
        auto r = static_cast<const benchmark::RelationTest *>(&record);
        EXPECT_EQ(idx * 5, r->data);
      });
    }
    db_->CommitTransaction();
  });
  db_->worker_pool.ScheduleSyncJob(0, [&]() {
    db_->StartTransaction();
    for (int idx = 0; idx < NO_RECORDS; idx++) {
      benchmark::RelationTest::Key key{idx};
      adapter->UpdateInPlace(key, [&idx](benchmark::RelationTest &record) {
        auto r  = static_cast<benchmark::RelationTest *>(&record);
        r->data = idx * 10;
      });
    }
    for (int idx = 0; idx < NO_RECORDS; idx++) {
      benchmark::RelationTest::Key key{idx};
      adapter->LookUp(key, [&idx](const benchmark::RelationTest &record) {
        auto r = static_cast<const benchmark::RelationTest *>(&record);
        EXPECT_EQ(idx * 10, r->data);
      });
    }
    db_->CommitTransaction();
  });

  db_->worker_pool.ScheduleSyncJob(1, [&]() {
    db_->StartTransaction();
    adapter->Scan(benchmark::RelationTest::Key{0},
                  [](const benchmark::RelationTest::Key &r_key, const benchmark::RelationTest &record) -> bool {
                    auto key = static_cast<const benchmark::RelationTest::Key *>(&r_key);
                    auto r   = static_cast<const benchmark::RelationTest *>(&record);
                    EXPECT_NE(key->primary_id, NO_RECORDS / 2 + 1);
                    EXPECT_EQ(key->primary_id * 10, r->data);
                    return key->primary_id < NO_RECORDS / 2;
                  });
    for (int idx = 0; idx < NO_RECORDS; idx++) {
      EXPECT_TRUE(adapter->Erase(benchmark::RelationTest::Key{idx}));
      EXPECT_FALSE(adapter->Erase(benchmark::RelationTest::Key{idx}));
    }
    EXPECT_EQ(adapter->Count(), 0);
    db_->CommitTransaction();
  });
}

TEST_F(TestLeanStoreAdapter, VariableSizeTest) {
  auto adapter = std::make_unique<LeanStoreAdapter<benchmark::VariableSizeRelation>>(*db_);

  db_->worker_pool.ScheduleSyncJob(0, [&]() {
    db_->StartTransaction();
    for (int idx = 0; idx < NO_RECORDS; idx++) {
      alignas(32) char storage[sizeof(Integer) + idx * sizeof(uint8_t)];
      benchmark::VariableSizeRelation::Key key{idx};
      auto payload = new (&storage[0]) benchmark::VariableSizeRelation(idx);
      for (auto i = 0; i < idx; i++) { payload->data[i] = idx; }
      adapter->Insert(key, *payload);
    }
    EXPECT_EQ(adapter->Count(), NO_RECORDS);
    db_->CommitTransaction();
  });

  db_->worker_pool.ScheduleSyncJob(1, [&]() {
    db_->StartTransaction();
    adapter->Scan(
      benchmark::VariableSizeRelation::Key{0},
      [](const benchmark::VariableSizeRelation::Key &r_key, const benchmark::VariableSizeRelation &record) -> bool {
        auto key = static_cast<const benchmark::VariableSizeRelation::Key *>(&r_key);
        auto r   = static_cast<const benchmark::VariableSizeRelation *>(&record);
        EXPECT_EQ(r->PayloadSize(), key->primary_id + sizeof(Integer));
        for (auto i = 0; i < r->size; i++) { EXPECT_EQ(key->primary_id, r->data[i]); }
        return key->primary_id < NO_RECORDS / 2;
      });
    for (int idx = 0; idx < NO_RECORDS; idx++) {
      EXPECT_TRUE(adapter->Erase(benchmark::VariableSizeRelation::Key{idx}));
      EXPECT_FALSE(adapter->Erase(benchmark::VariableSizeRelation::Key{idx}));
    }
    EXPECT_EQ(adapter->Count(), 0);
    db_->CommitTransaction();
  });
}

}  // namespace leanstore

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_db_path                 = BLOCK_DEVICE;
  FLAGS_bm_aio_qd               = 8;
  FLAGS_worker_count            = 4;
  FLAGS_wal_stealing_group_size = 1;
  FLAGS_txn_commit_group_size   = 1;
  leanstore::RegisterSEGFAULTHandler();

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}