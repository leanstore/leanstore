#include "storage/blob/aliasing_guard.h"

namespace leanstore::storage::blob {

AliasingGuard::AliasingGuard(buffer::BufferManager *buffer, const BlobState &blob, u64 required_load_size)
    : buffer_(buffer) {
  // FLAGS_blob_normal_buffer_pool: 2nd extra overhead
  if (FLAGS_blob_normal_buffer_pool) {
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
      Ensure(idx++ == blob.extents.NumberOfExtents());
      auto copy_size = std::min(required_load_size - offset, blob.extents.tail.page_cnt * PAGE_SIZE);
      buffer->ChunkOperation(blob.extents.tail.start_pid, copy_size, [&](u64 off, std::span<u8> payload) {
        std::memcpy(&ptr_[offset + off], payload.data(), payload.size());
      });
      offset += copy_size;
    }
    Ensure(offset >= required_load_size);
    return;
  }

  // Create Alias working area
  ptr_ = reinterpret_cast<u8 *>(buffer_->AliasArea()->RequestAliasingArea(blob.blob_size));

  // Prepare the aliasing params
  u64 alias_size = 0;
  size_t idx     = 0;
  for (; (idx < blob.extents.NumberOfExtents()) && (alias_size < required_load_size); idx++) {
    auto pg_cnt                                               = ExtentList::ExtentSize(idx);
    buffer->exmap_interface_[worker_thread_id]->iov[idx].page = blob.extents.extent_pid[idx];
    buffer->exmap_interface_[worker_thread_id]->iov[idx].len  = pg_cnt;
    alias_size += pg_cnt * PAGE_SIZE;
  }
  if (blob.extents.tail_in_used && alias_size < required_load_size) {
    Ensure(idx++ == blob.extents.NumberOfExtents());
    buffer->exmap_interface_[worker_thread_id]->iov[idx - 1].page = blob.extents.tail.start_pid;
    buffer->exmap_interface_[worker_thread_id]->iov[idx - 1].len  = blob.extents.tail.page_cnt;
    alias_size += blob.extents.tail.page_cnt * PAGE_SIZE;
  }
  Ensure(alias_size >= required_load_size);

  // Aliasing the whole blob
  struct exmap_action_params params = {
    .interface = static_cast<u16>(worker_thread_id),
    .iov_len   = static_cast<u16>(idx),
    .opcode    = static_cast<u16>(EXMAP_OP_SHADOW),
    .page_id   = buffer->ToPID(ptr_),
  };

  // Execute Exmap SHADOW operation
  Ensure(ioctl(buffer->exmapfd_, EXMAP_IOCTL_ACTION, &params) >= 0);
}

AliasingGuard::~AliasingGuard() {
  if (FLAGS_blob_normal_buffer_pool) {
    free(ptr_);
  } else {
    ExmapAction(buffer_->exmapfd_, EXMAP_OP_RM_SD, 0);
    buffer_->AliasArea()->ReleaseAliasingArea();
  }
}

auto AliasingGuard::GetPtr() -> u8 * { return ptr_; }

}  // namespace leanstore::storage::blob