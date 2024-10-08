#pragma once

#include "buffer/buffer_manager.h"
#include "storage/blob/blob_state.h"

#include "gtest/gtest_prod.h"
#include "share_headers/db_types.h"

#include <memory>
#include <span>

namespace leanstore::storage::blob {

// RAII BLOB aliasing guard
struct AliasingGuard {
  AliasingGuard(buffer::BufferManager *buffer, const BlobState &blob, u64 required_load_size);
  ~AliasingGuard();
  auto GetPtr() -> u8 *;

 private:
  u8 *ptr_{nullptr};
  buffer::BufferManager *buffer_;
};

}  // namespace leanstore::storage::blob