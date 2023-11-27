#include "storage/aio.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"

#include "share_headers/logger.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace leanstore::storage {

LibaioInterface::LibaioInterface(int blockfd, Page *virtual_mem) : blockfd_(blockfd), virtual_mem_(virtual_mem) {
  assert(FLAGS_bm_aio_qd <= MAX_IOS);
  int ret = io_uring_queue_init(FLAGS_bm_aio_qd, &ring_, 0);
  if (ret != 0) { throw std::runtime_error("GroupCommit: io_uring_queue_init error"); }
}

void LibaioInterface::UringSubmit(size_t submit_cnt) {
  if (submit_cnt > 0) {
    [[maybe_unused]] auto ret = io_uring_submit(&ring_);
    assert(ret == static_cast<int>(submit_cnt));
    struct io_uring_cqe *cqes[submit_cnt];
    io_uring_wait_cqe_nr(&ring_, cqes, submit_cnt);
    io_uring_cq_advance(&ring_, submit_cnt);
  }
}

void LibaioInterface::WritePages(std::vector<pageid_t> &pages) {
  u32 submit_cnt = 0;

  for (auto pid : pages) {
    auto sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      UringSubmit(submit_cnt);
      sqe = io_uring_get_sqe(&ring_);
      Ensure(sqe != nullptr);
      submit_cnt = 0;
    }
    submit_cnt++;
    io_uring_prep_write(sqe, blockfd_, &virtual_mem_[pid], PAGE_SIZE, PAGE_SIZE * pid);
  }
  UringSubmit(submit_cnt);
}

/**
 * @brief Read a list of huge pages
 */
void LibaioInterface::ReadLargePages(const LargePageList &large_pages) {
  u32 submit_cnt = 0;
  for (auto &large_pg : large_pages) {
    auto sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      UringSubmit(submit_cnt);
      sqe = io_uring_get_sqe(&ring_);
      Ensure(sqe != nullptr);
      submit_cnt = 0;
    }
    submit_cnt++;
    io_uring_prep_read(sqe, blockfd_, &virtual_mem_[large_pg.start_pid], PAGE_SIZE * large_pg.page_cnt,
                       PAGE_SIZE * large_pg.start_pid);
  }
  UringSubmit(submit_cnt);
}

}  // namespace leanstore::storage