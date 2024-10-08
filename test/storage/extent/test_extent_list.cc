#include "common/typedefs.h"
#include "storage/extent/extent_list.h"

#include "gtest/gtest.h"
#include "share_headers/logger.h"

#include <array>
#include <memory>

namespace leanstore::storage {

class TestExtentList : public ::testing::Test {
 protected:
  ExtentList *ext_;
  std::array<u8, ExtentList::MallocSize(ExtentList::EXTENT_CNT_MASK)> storage_;

  void SetUp() override { ext_ = new (storage_.data()) ExtentList(); }
};

TEST_F(TestExtentList, BasicTest) {
  u64 expected_total_size = 0;

  // Extent Info Calculation
  for (auto idx = 0; idx < 100; idx++) {
    expected_total_size += ExtentList::ExtentSize(idx);
    EXPECT_EQ(expected_total_size, ExtentList::TotalSizeExtents(idx));
  }

  for (auto idx = 1; idx <= 100; idx++) {
    auto test_size  = ExtentList::TotalSizeExtents(idx - 1) + 1;
    auto no_extents = ExtentList::NoSpanExtents(test_size);
    EXPECT_EQ(no_extents, idx + 1);
    auto payload_size = test_size;
    for (u8 idx = 0; idx < no_extents - 1; idx++) {
      EXPECT_GT(payload_size, ExtentList::ExtentSize(idx));
      payload_size -= ExtentList::ExtentSize(idx);
    }
    EXPECT_LE(payload_size, ExtentList::ExtentSize(no_extents - 1));
  }

  // Extent ops
  ext_->extent_cnt = 127;
  EXPECT_EQ(ext_->NumberOfExtents(), 127);
  ext_->extent_cnt = 0;
  EXPECT_EQ(ext_->NumberOfExtents(), 0);
  ext_->extent_cnt = 127;
  EXPECT_EQ(ext_->NumberOfExtents(), 127);
}

}  // namespace leanstore::storage