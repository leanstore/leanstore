#include "benchmark/adapters/wiredtiger_adapter.h"
#include "benchmark/utils/test_utils.h"
#include "test/base_test.h"

#include "gtest/gtest.h"
#include "share_headers/db_types.h"

#include <filesystem>

static constexpr int NO_RECORDS = 1000000;

class TestWiredTigerAdapter : public ::testing::Test {
 protected:
  std::unique_ptr<WiredTigerDB> db_;
  std::unique_ptr<WiredTigerAdapter<benchmark::RelationTest>> adapter_;

  void SetUp() override {
    FLAGS_db_path = "/tmp/wiredtiger-test";

    db_ = std::make_unique<WiredTigerDB>();
    db_->PrepareThread();
    adapter_ = std::make_unique<WiredTigerAdapter<benchmark::RelationTest>>(*db_);
  }

  void TearDown() override {
    adapter_.reset();
    db_->CloseSession();
    db_.reset();
    std::filesystem::remove_all(FLAGS_db_path);
  }
};

TEST_F(TestWiredTigerAdapter, BasicTest) {
  db_->StartTransaction();

  for (int idx = 0; idx < NO_RECORDS; idx++) {
    benchmark::RelationTest::Key key{idx};
    benchmark::RelationTest data{idx * 5};
    adapter_->Insert(key, data);
  }
  EXPECT_EQ(adapter_->Count(), NO_RECORDS);
  for (int idx = 0; idx < NO_RECORDS; idx++) {
    benchmark::RelationTest::Key key{idx};
    adapter_->LookUp(key, [&idx](const benchmark::RelationTest &record) {
      auto r = static_cast<const benchmark::RelationTest *>(&record);
      EXPECT_EQ(idx * 5, r->data);
    });
  }

  db_->CommitTransaction();

  db_->StartTransaction();
  for (int idx = 0; idx < NO_RECORDS; idx++) {
    benchmark::RelationTest::Key key{idx};
    adapter_->UpdateInPlace(key, [&idx](benchmark::RelationTest &record) {
      auto r  = static_cast<benchmark::RelationTest *>(&record);
      r->data = idx * 10;
    });
  }
  for (int idx = 0; idx < NO_RECORDS; idx++) {
    benchmark::RelationTest::Key key{idx};
    adapter_->LookUp(key, [&idx](const benchmark::RelationTest &record) {
      auto r = static_cast<const benchmark::RelationTest *>(&record);
      EXPECT_EQ(idx * 10, r->data);
    });
  }
  db_->CommitTransaction();

  db_->StartTransaction();
  adapter_->Scan(benchmark::RelationTest::Key{0},
                 [](const benchmark::RelationTest::Key &r_key, const benchmark::RelationTest &record) -> bool {
                   auto key = static_cast<const benchmark::RelationTest::Key *>(&r_key);
                   auto r   = static_cast<const benchmark::RelationTest *>(&record);
                   EXPECT_NE(key->primary_id, NO_RECORDS / 2 + 1);
                   EXPECT_EQ(key->primary_id * 10, r->data);
                   return key->primary_id < NO_RECORDS / 2;
                 });
  for (int idx = 0; idx < NO_RECORDS; idx++) {
    EXPECT_TRUE(adapter_->Erase(benchmark::RelationTest::Key{idx}));
    EXPECT_FALSE(adapter_->Erase(benchmark::RelationTest::Key{idx}));
  }
  EXPECT_EQ(adapter_->Count(), 0);
  db_->CommitTransaction();
}

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  auto test_path = fs::path(testing::TempDir()) / fs::path("wiredtiger-test.db");

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}