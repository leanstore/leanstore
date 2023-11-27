#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "storage/extent/large_page.h"
#include "storage/page.h"

#include "liburing.h"

namespace leanstore::storage {

class LibaioInterface {
  static constexpr u32 MAX_IOS = 1024;

 public:
  LibaioInterface(int blockfd, Page *virtual_mem);
  ~LibaioInterface() = default;
  void WritePages(std::vector<pageid_t> &pages);
  void ReadLargePages(const LargePageList &large_pages);

 private:
  int blockfd_;
  Page *virtual_mem_;

  /* io_uring properties */
  struct io_uring ring_;
  void UringSubmit(size_t submit_cnt);
};

}  // namespace leanstore::storage