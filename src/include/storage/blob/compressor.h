#pragma once

#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"

#include "gtest/gtest_prod.h"

#include "fsst.h"
#include "lz4.h"
#include "zstd.h"

#include <cstdlib>
#include <span>

namespace leanstore::storage::blob {

// TODO(Duy): If compressed data is very small (the borderline to be determined later)
//  then we should store that compressed data instead of BlobTuple & Blob allocation
class BlobCompressor {
 public:
  fsst_encoder_t *fsst = nullptr;  // Only used in case of FSST
  u8 *buffer           = nullptr;
  u64 size             = 0;
  u64 max_buffer_sz    = 0;

  BlobCompressor() = default;
  ~BlobCompressor();

  void Reset(u64 expect_size);
  void CompressBlob(std::span<const u8> blob);
  auto ToSpan() -> std::span<const u8>;

  // All internal compress calls
  void FSSTCompressBlob(const std::span<const u8> &blob);
  void ZSTDCompressBlob(const std::span<const u8> &blob);
  void LZ4CompressBlob(const std::span<const u8> &blob);

 private:
  FRIEND_TEST(TestCompression, BlobCompressor);
  void DestroyBuffer();
};

}  // namespace leanstore::storage::blob