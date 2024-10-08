#include "common/typedefs.h"
#include "storage/aio.h"
#include "storage/page.h"
#include "test/base_test.h"

#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include <sys/mman.h>
#include <cstdio>
#include <ranges>

namespace leanstore {

class TestAioInterface : public BaseTest {
 protected:
  storage::Page *mem_;
  static constexpr u64 ALLOC_SIZE = PAGE_SIZE * 4 + (1 << 16);

  TestAioInterface() = default;

  void SetUp() override {
    BaseTest::SetupTestFile(true);
    LOG_INFO("Alloc size %lu", ALLOC_SIZE);
    mem_ = static_cast<storage::Page *>(
      mmap(nullptr, ALLOC_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    madvise(mem_, ALLOC_SIZE, MADV_NOHUGEPAGE);
  }

  void TearDown() override {
    munmap(mem_, ALLOC_SIZE);
    BaseTest::TearDown();
  }
};

TEST_F(TestAioInterface, BasicTest) {
  auto aio_inf = storage::LibaioInterface(test_file_fd_, mem_);

  // Write Pages 0, 1, and 3. Page 0 is empty
  auto dirty_pages = std::vector<pageid_t>{0, 1, 3};
  for (auto pid : dirty_pages) { ModifyPageContent(&mem_[pid]); }
  std::memset(reinterpret_cast<u8 *>(&mem_[0]), 0, PAGE_SIZE);
  aio_inf.WritePages(dirty_pages);

  // Read from the test file using mmap
  auto data = reinterpret_cast<u8 *>(mmap(nullptr, 4 * PAGE_SIZE, PROT_READ, MAP_PRIVATE, test_file_fd_, 0));

  // Content of Page 0 should be 0
  for (size_t idx = 0; idx < PAGE_SIZE; ++idx) { EXPECT_EQ(data[idx], 0); }

  // Validate the content of dirty pages
  for (auto pid : std::ranges::remove(dirty_pages, 0)) {
    for (size_t idx = sizeof(storage::PageHeader); idx < PAGE_SIZE; ++idx) {
      EXPECT_EQ(data[pid * PAGE_SIZE + idx], 111);
    }
  }
}

}  // namespace leanstore