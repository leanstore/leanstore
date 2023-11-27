#include "common/typedefs.h"
#include "storage/blob/compressor.h"

#include "gtest/gtest.h"

#include <cstdlib>

namespace leanstore::storage::blob {

TEST(TestCompression, BlobCompressor) {
  BlobCompressor blob;
  uint8_t data[1024]               = {0};
  std::span<const uint8_t> payload = {data, 1024};

  blob.FSSTCompressBlob(payload);
  EXPECT_EQ(blob.ToSpan().size() % BLK_BLOCK_SIZE, 0);

  blob.ZSTDCompressBlob(payload);
  EXPECT_EQ(blob.ToSpan().size() % BLK_BLOCK_SIZE, 0);

  blob.LZ4CompressBlob(payload);
  EXPECT_EQ(blob.ToSpan().size() % BLK_BLOCK_SIZE, 0);
}

}  // namespace leanstore::storage::blob