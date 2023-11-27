#include "storage/blob/compressor.h"
#include "common/utils.h"
#include "leanstore/config.h"

namespace leanstore::storage::blob {

BlobCompressor::~BlobCompressor() { DestroyBuffer(); }

void BlobCompressor::DestroyBuffer() {
  size          = 0;
  max_buffer_sz = 0;
  if (fsst != nullptr) {
    fsst_destroy(fsst);
    fsst = nullptr;
  }
  if (buffer != nullptr) {
    free(buffer);
    buffer = nullptr;
  }
}

void BlobCompressor::Reset(u64 expect_size) {
  DestroyBuffer();
  Ensure(buffer == nullptr);
  Ensure(IsAligned(BLK_BLOCK_SIZE, nullptr, expect_size));
  max_buffer_sz = expect_size;
  buffer        = static_cast<u8 *>(aligned_alloc(BLK_BLOCK_SIZE, max_buffer_sz));
}

void BlobCompressor::CompressBlob(std::span<const u8> blob) {
  // Alloc Encoder and serialize it to buffer
  switch (FLAGS_blob_compression) {
    case 1: FSSTCompressBlob(blob); break;
    case 2: ZSTDCompressBlob(blob); break;
    case 3: LZ4CompressBlob(blob); break;
    default: UnreachableCode();
  }
}

auto BlobCompressor::ToSpan() -> std::span<const u8> {
  assert(UpAlign(size) <= max_buffer_sz);
  return std::span<const u8>{buffer, UpAlign(size)};
}

void BlobCompressor::FSSTCompressBlob(const std::span<const u8> &blob) {
  size_t compressed_size;
  u8 *unused[1];
  u8 tmp[FSST_MAXHEADER];
  size_t blob_size[1] = {blob.size()};
  u8 *blob_ptr[1]     = {const_cast<u8 *>(blob.data())};

  // Allocate buffer & generate encoder
  Reset(UpAlign(FSST_MAXHEADER + blob.size() * 2 + 7));
  fsst = fsst_create(1, blob_size, blob_ptr, 0);
  size = fsst_export(fsst, tmp);

  // Serialize the fsst encoder to buffer [encoder's size, encoder's content]
  std::memcpy(buffer, &size, sizeof(leng_t));
  std::memcpy(&buffer[sizeof(leng_t)], tmp, size);
  size += sizeof(leng_t);

  // Compress the main blob to buffer
  auto ret = fsst_compress(fsst, 1, blob_size, blob_ptr, max_buffer_sz - size, &buffer[size], &compressed_size, unused);
  Ensure(ret >= 1);
  size += compressed_size;
}

void BlobCompressor::ZSTDCompressBlob(const std::span<const u8> &blob) {
  Reset(UpAlign(ZSTD_compressBound(blob.size())));
  size = ZSTD_compress(buffer, max_buffer_sz, blob.data(), blob.size(), FLAGS_zstd_compress_level);
  Ensure(!ZSTD_isError(size));
}

void BlobCompressor::LZ4CompressBlob(const std::span<const u8> &blob) {
  Reset(UpAlign(LZ4_compressBound(blob.size())));
  size = LZ4_compress_fast(reinterpret_cast<const char *>(blob.data()), reinterpret_cast<char *>(buffer), blob.size(),
                           max_buffer_sz, FLAGS_lz4_compress_acceleration);
  Ensure(size);
}

}  // namespace leanstore::storage::blob