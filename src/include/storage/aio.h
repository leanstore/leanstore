#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "storage/extent/large_page.h"
#include "storage/page.h"

#include "liburing.h"

namespace leanstore::storage {

class LibaioInterface {
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
};

}  // namespace leanstore::storage