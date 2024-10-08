#include "prototype/free_page/blob_alloc.h"
#include "prototype/free_page/impl.h"

#include "gtest/gtest.h"

#include <set>

TEST(FirstFit, Basic) {
  auto no_pages = TIER_SIZE(8);
  auto man      = FirstFitAllocator(no_pages);

  for (auto tier = 0; tier <= 8; tier++) {
    size_t out_pid = 0;
    for (auto idx = 0; idx < no_pages / TIER_SIZE(tier); idx++) {
      auto found = man.AllocTierPage(tier, out_pid);
      EXPECT_TRUE(found);
      EXPECT_EQ(out_pid, idx * TIER_SIZE(tier));
    }
    auto found = man.AllocTierPage(tier, out_pid);
    EXPECT_FALSE(found);
    man.DeallocTierPage(0, 8);
  }
}

TEST(BitmapTest, Basic) {
  auto no_pages = TIER_SIZE(8);
  auto man      = BitmapFreePages(no_pages);

  for (auto tier = 0; tier <= 8; tier++) {
    size_t out_pid = 0;
    for (auto idx = 0; idx < no_pages / TIER_SIZE(tier); idx++) {
      auto found = man.AllocTierPage(tier, out_pid);
      EXPECT_TRUE(found);
      EXPECT_EQ(out_pid, idx * TIER_SIZE(tier));
    }
    auto found = man.AllocTierPage(tier, out_pid);
    EXPECT_FALSE(found);
    for (auto idx = 0; idx < no_pages / TIER_SIZE(tier); idx++) { man.DeallocTierPage(idx * TIER_SIZE(tier), tier); }
    for (auto &map : man.bitmap) { EXPECT_EQ(map.to_string(), std::string(64, '1')); }
  }
}

TEST(TreemapTest, Basic) {
  auto no_pages = TIER_SIZE(8);
  auto man      = TreeFreePages(no_pages);

  for (auto tier = 0; tier <= 8; tier++) {
    size_t out_pid = 0;
    for (auto idx = 0; idx < no_pages / TIER_SIZE(tier); idx++) {
      auto found = man.AllocTierPage(tier, out_pid);
      EXPECT_TRUE(found);
      EXPECT_EQ(out_pid, idx * TIER_SIZE(tier));
    }
    auto found = man.AllocTierPage(tier, out_pid);
    EXPECT_FALSE(found);
    for (auto idx = 0; idx < no_pages / TIER_SIZE(tier); idx++) { man.DeallocTierPage(idx * TIER_SIZE(tier), tier); }
  }
}

TEST(TieringTest, Basic) {
  auto no_pages = TIER_SIZE(8);
  auto man      = TieringFreePages<8>(TIER_SIZE(8));

  for (auto tier = 0; tier <= 8; tier++) {
    size_t pid;
    std::set<size_t> out_pids;
    for (auto idx = 0; idx < no_pages / TIER_SIZE(tier); idx++) {
      auto found = man.AllocTierPage(tier, pid);
      if (!found) { fmt::print("idx {} - tier {}\n", idx, tier); }
      EXPECT_TRUE(found);
      auto [pos, success] = out_pids.insert(pid);
      EXPECT_TRUE(success);
    }
    EXPECT_EQ(out_pids.size(), no_pages / TIER_SIZE(tier));
    auto idx = 0;
    for (auto &pid : out_pids) {
      EXPECT_EQ(pid, idx * TIER_SIZE(tier));
      idx++;
    }
    auto found = man.AllocTierPage(tier, pid);
    EXPECT_FALSE(found);
    man.DeallocTierPage(0, 8);
  }
}

TEST(BlobAllocWithFirstFit, Basic) {
  auto man = FirstFitAllocator(TIER_SIZE(BlobRep::MAX_TIER + 3));
  auto buf = BufferManager(&man);

  auto success = buf.AllocBlob(TIER_SIZE(BlobRep::MAX_TIER + 3));
  EXPECT_FALSE(success);
  EXPECT_EQ(buf.BlobCount(), 0);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);

  success = buf.AllocBlob(TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  EXPECT_TRUE(success);
  EXPECT_EQ(buf.BlobCount(), 1);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);

  buf.RemoveBlob(0);
  EXPECT_EQ(buf.BlobCount(), 0);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);

  for (auto idx = 1; idx < BlobRep::MAX_TIER; idx++) {
    success = buf.AllocBlob(TIER_SIZE(idx));
    EXPECT_TRUE(success);
    EXPECT_EQ(reinterpret_cast<BlobRep *>(buf.blob_list.front())->extent_cnt, idx + 1);
    EXPECT_EQ(buf.BlobCount(), 1);

    buf.RemoveBlob(0);
    EXPECT_EQ(buf.BlobCount(), 0);
  }
}

TEST(BlobAllocWithBitmap, Basic) {
  auto man = BitmapFreePages(TIER_SIZE(BlobRep::MAX_TIER + 3));
  auto buf = BufferManager(&man);

  auto success = buf.AllocBlob(TIER_SIZE(BlobRep::MAX_TIER + 3));
  EXPECT_FALSE(success);
  EXPECT_EQ(buf.BlobCount(), 0);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  for (size_t idx = 0; idx < man.bitmap.size() - 1; idx++) { ASSERT_EQ(man.bitmap[idx], std::bitset<64>(ULONG_MAX)); }

  success = buf.AllocBlob(TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  EXPECT_TRUE(success);
  EXPECT_EQ(buf.BlobCount(), 1);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);

  buf.RemoveBlob(0);
  EXPECT_EQ(buf.BlobCount(), 0);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  for (size_t idx = 0; idx < man.bitmap.size() - 1; idx++) { ASSERT_EQ(man.bitmap[idx], std::bitset<64>(ULONG_MAX)); }

  for (auto idx = 1; idx < BlobRep::MAX_TIER; idx++) {
    success = buf.AllocBlob(TIER_SIZE(idx));
    EXPECT_TRUE(success);
    EXPECT_EQ(reinterpret_cast<BlobRep *>(buf.blob_list.front())->extent_cnt, idx + 1);
    EXPECT_EQ(buf.BlobCount(), 1);

    buf.RemoveBlob(0);
    EXPECT_EQ(buf.BlobCount(), 0);
  }
}

TEST(BlobAllocWithTree, Basic) {
  auto man = TreeFreePages(TIER_SIZE(BlobRep::MAX_TIER + 3));
  auto buf = BufferManager(&man);

  auto success = buf.AllocBlob(TIER_SIZE(BlobRep::MAX_TIER + 3));
  EXPECT_FALSE(success);
  EXPECT_EQ(buf.BlobCount(), 0);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  EXPECT_EQ(man.free_pages.size(), man.free_start_pid.size());

  success = buf.AllocBlob(TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  EXPECT_TRUE(success);
  EXPECT_EQ(buf.BlobCount(), 1);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  EXPECT_EQ(man.free_pages.size(), man.free_start_pid.size());

  buf.RemoveBlob(0);
  EXPECT_EQ(buf.BlobCount(), 0);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  EXPECT_EQ(man.free_pages.size(), man.free_start_pid.size());

  for (auto idx = 1; idx < BlobRep::MAX_TIER; idx++) {
    success = buf.AllocBlob(TIER_SIZE(idx));
    EXPECT_TRUE(success);
    EXPECT_EQ(reinterpret_cast<BlobRep *>(buf.blob_list.front())->extent_cnt, idx + 1);
    EXPECT_EQ(buf.BlobCount(), 1);
    EXPECT_EQ(man.free_pages.size(), man.free_start_pid.size());

    buf.RemoveBlob(0);
    EXPECT_EQ(buf.BlobCount(), 0);
    EXPECT_EQ(man.free_pages.size(), man.free_start_pid.size());
  }
}

TEST(BlobAllocWithTiering, Basic) {
  auto man = TieringFreePages<BlobRep::MAX_TIER>(TIER_SIZE(BlobRep::MAX_TIER + 3));
  auto buf = BufferManager(&man);

  auto success = buf.AllocBlob(TIER_SIZE(BlobRep::MAX_TIER + 3));
  EXPECT_FALSE(success);
  EXPECT_EQ(buf.BlobCount(), 0);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);

  success = buf.AllocBlob(TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);
  EXPECT_TRUE(success);
  EXPECT_EQ(buf.BlobCount(), 1);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);

  buf.RemoveBlob(0);
  EXPECT_EQ(buf.BlobCount(), 0);
  EXPECT_EQ(man.alloc_cnt, TIER_SIZE(BlobRep::MAX_TIER + 3) - 1);

  for (auto idx = 1; idx < BlobRep::MAX_TIER; idx++) {
    success = buf.AllocBlob(TIER_SIZE(idx));
    EXPECT_TRUE(success);
    EXPECT_EQ(reinterpret_cast<BlobRep *>(buf.blob_list.front())->extent_cnt, idx + 1);
    EXPECT_EQ(buf.BlobCount(), 1);

    buf.RemoveBlob(0);
    EXPECT_EQ(buf.BlobCount(), 0);
  }
}

auto main() -> int {
  testing::InitGoogleTest();
  return RUN_ALL_TESTS();
}