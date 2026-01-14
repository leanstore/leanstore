#include "storage/blob/aliasing_guard.h"
#include "leanstore/leanstore.h"

namespace leanstore::storage::blob {

AliasingGuard::AliasingGuard(buffer::BufferManager *buffer, const BlobState &blob, u64 required_load_size)
    : buffer_(buffer) {
  ptr_       = reinterpret_cast<u8 *>(malloc(required_load_size));
  u64 offset = 0;
  size_t idx = 0;
  for (; (idx < blob.extents.NumberOfExtents()) && (offset < required_load_size); idx++) {
    auto copy_size = std::min(required_load_size - offset, ExtentList::ExtentSize(idx) * PAGE_SIZE);
    buffer->ChunkOperation(blob.extents.extent_pid[idx], copy_size, [&](u64 off, std::span<u8> payload) {
      std::memcpy(&ptr_[offset + off], payload.data(), payload.size());
    });
    offset += copy_size;
  }
  if (blob.extents.tail_in_used && offset < required_load_size) {
    auto copy_size = std::min(required_load_size - offset, blob.extents.tail.page_cnt * PAGE_SIZE);
    buffer->ChunkOperation(blob.extents.tail.start_pid, copy_size, [&](u64 off, std::span<u8> payload) {
      std::memcpy(&ptr_[offset + off], payload.data(), payload.size());
    });
    offset += copy_size;
  }
  Ensure(offset >= required_load_size);
}

AliasingGuard::~AliasingGuard() { free(ptr_); }

auto AliasingGuard::GetPtr() -> u8 * { return ptr_; }

}  // namespace leanstore::storage::blob