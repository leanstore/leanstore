#include "benchmark/adapters/filesystem_adapter.h"
#include "benchmark/utils/test_utils.h"

#include "gtest/gtest.h"
#include "share_headers/db_types.h"

#include <filesystem>

static constexpr int NO_RECORDS = 10;

class TestFilesystemAdapter : public ::testing::Test {
 protected:
  std::unique_ptr<FilesystemAsDB> db_;
  std::unique_ptr<FilesystemAdapter<benchmark::RelationTest>> adapter_;

  void SetUp() override {
    FLAGS_db_path = "/tmp/fs-test";
    db_           = std::make_unique<FilesystemAsDB>(FLAGS_db_path, true);
    adapter_      = std::make_unique<FilesystemAdapter<benchmark::RelationTest>>(db_.get());
  }

  void TearDown() override {
    adapter_.reset();
    db_.reset();
    std::filesystem::remove_all(FLAGS_db_path);
  }
};

TEST_F(TestFilesystemAdapter, BasicTest) {
  for (int idx = 0; idx < NO_RECORDS; idx++) {
    benchmark::RelationTest::Key key{idx};
    benchmark::RelationTest data{idx * 5};
    adapter_->Insert(key, data);
  }
  EXPECT_EQ(adapter_->Count(), 10);
  for (int idx = 0; idx < NO_RECORDS; idx++) {
    benchmark::RelationTest::Key key{idx};
    adapter_->LookUp(key, [&idx](const benchmark::RelationTest &record) {
      auto r = static_cast<const benchmark::RelationTest *>(&record);
      EXPECT_EQ(idx * 5, r->data);
    });
  }

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
}

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}