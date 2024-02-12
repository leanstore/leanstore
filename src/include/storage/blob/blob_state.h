#pragma once

#include "common/constants.h"
#include "common/sha256.h"
#include "storage/extent/extent_list.h"

#include "share_headers/db_types.h"
#include "share_headers/logger.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <span>

namespace leanstore::storage::blob {

/**
 * @brief The data struct to be stored within DB tuple, which refers to the Blob
 * A single Blob can only be referred to by exactly one BlobState,
 *  and all Blobs whose have the same root_id are considered to be the same Blob
 *
 * Theoretically, as long as a Page can contain at least two Blob States, then Index will work
 * In other words, a Blob Handler could be approx as big as (PAGE_SIZE - PAGE_HEADER_SIZE) / 2
 * We can safely assume a MAX_MALLOC_SIZE of 1700 bytes, which corresp. to (1700 - MIN_MALLOC_SIZE) / 8 = 202 extents
 * Using the Power-of-Two formula, i.e. when ExtentList::EXTENT_PER_LEVEL = 202 and ExtentList::NUMBER_OF_LEVELS = 1
 *  The maximum BLOB size is 2.18 * 10^40 YB
 *
 * Here, we want set a limit of 127 extents (7 bits, 1 reserved bit for later use) per Blob Handler.
 * Therefore, the maximum malloc size is 1100 bytes.
 * Advantages:
 * - Beautiful number. Who doesn't like perfection?
 * - Each page can store at least 3 Blob States, i.e. more efficient indexing
 * Maximum BLOB size in this case is 576460752303423488 YB ~ 5.76 * 10^17 YB
 */
struct BlobState {
  static constexpr uint8_t PREFIX_LENGTH         = 32;
  static constexpr uint16_t MIN_MALLOC_SIZE      = 120UL;   // MallocSize(0)
  static constexpr uint16_t MAX_MALLOC_SIZE      = 1136UL;  // MallocSize(ExtentList::EXTENT_CNT_MASK)
  static constexpr uint16_t SHA256_DIGEST_LENGTH = 32;
  static thread_local SHA256H sha_context;

  // Having `blob_prefix` as the 1st property eases B-Tree impl
  uint8_t blob_prefix[PREFIX_LENGTH] = {0};      // Prefix of the BLOB for ordered/range operators
  uint64_t blob_size;                            // Size of this blob in bytes
  uint8_t sha2_val[SHA256_DIGEST_LENGTH] = {0};  // SHA-2 value, used for hash operators

  // Intermediate SHA value for resumable SHA-256 calculation
  uint64_t sha256_intermediate[4];

  // Information about Blob physical content
  ExtentList extents;

  // -------------------------------------------------------------------------------------
  auto operator!=(const BlobState &other) -> bool { return BlobID() != other.BlobID(); }

  auto BlobID() const -> UniqueID {
    assert(extents.NumberOfExtents() > 0);
    return extents.extent_pid[0];
  }

  // Utility to access the content directly in LeanStoreAdapter
  auto Data() -> u8 * { return reinterpret_cast<u8 *>(this); }

  // -------------------------------------------------------------------------------------
  /**
   * @brief Size of a BlobState with specified number of extents
   */
  static constexpr auto MallocSize(uint8_t no_extents) -> uint16_t {
    return sizeof(BlobState) + sizeof(pageid_t) * no_extents;
  }

  static auto PageCount(uint64_t req_size) -> uint64_t { return std::ceil(static_cast<float>(req_size) / PAGE_SIZE); }

  static auto MoveToTempStorage(u8 *tmp, BlobState *tmp_btup) -> BlobState * {
    std::memcpy(tmp, tmp_btup, tmp_btup->MallocSize());
    return reinterpret_cast<BlobState *>(tmp);
  }

  static void CalculateSHA256(uint8_t *sha2_digest, std::span<const uint8_t> payload) {
    sha_context.Initialize();
    sha_context.Update(payload.data(), payload.size());
    sha_context.Final(sha2_digest);
  }

  // -------------------------------------------------------------------------------------
  auto MallocSize() const -> uint16_t { return MallocSize(extents.NumberOfExtents()); }

  auto PageCount() const -> uint64_t { return PageCount(blob_size); }

  auto RemainBytesInLastExtent() const -> uint64_t {
    return ExtentList::TotalSizeExtents(ExtentList::NoSpanExtents(PageCount()) - 1) * PAGE_SIZE - blob_size;
  }

  void CalculateSHA256(std::span<const uint8_t> payload) {
    CalculateSHA256(sha2_val, payload);
    sha_context.Serialize(&sha256_intermediate[0]);
  }
};

struct BlobLookupKey {
  std::span<const u8> blob;
  uint8_t sha2_digest[BlobState::SHA256_DIGEST_LENGTH] = {0};  // SHA-2 value, used for hash operators

  explicit BlobLookupKey(std::span<const u8> blob_data) : blob(blob_data) {
    if (blob.size() > BlobState::PREFIX_LENGTH) { BlobState::CalculateSHA256(sha2_digest, blob); }
  }
};

// Initial size of BlobState should be fixed
static_assert(sizeof(BlobState) == BlobState::MIN_MALLOC_SIZE);
static_assert(BlobState::MallocSize(0) == BlobState::MIN_MALLOC_SIZE);
static_assert(BlobState::MallocSize(ExtentList::EXTENT_CNT_MASK) == BlobState::MAX_MALLOC_SIZE);

}  // namespace leanstore::storage::blob