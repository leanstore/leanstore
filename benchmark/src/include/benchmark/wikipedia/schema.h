#pragma once

#include "benchmark/utils/shared_schema.h"
#include "leanstore/leanstore.h"

#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <cstring>

namespace wiki {

static constexpr auto INDEX_PREFIX_LENGTH = 1024UL;  // 1 KB
static constexpr auto SHA2_LENGTH         = leanstore::BlobState::SHA256_DIGEST_LENGTH;

struct BlobStateIndex {
  static constexpr uint32_t TYPE_ID = 1;

  struct Key {
    uint8_t value[leanstore::BlobState::MAX_MALLOC_SIZE];
  };

  uint64_t article_id;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(article_id); }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto size = reinterpret_cast<const leanstore::BlobState *>(key.value)->MallocSize();
    std::memcpy(out, key.value, size);
    return size;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto size = reinterpret_cast<const leanstore::BlobState *>(in)->MallocSize();
    std::memcpy(key.value, in, size);
    return size;
  }

  static auto MaxFoldLength() -> uint32_t { return leanstore::BlobState::MAX_MALLOC_SIZE; }
};

template <uint32_t PrefixLength>
struct PrefixIndex {
  static constexpr uint32_t TYPE_ID = 2;

  struct Key {
    UInteger prefix_length;
    uint8_t value[PrefixLength];  // Index prefix of the payload
  };

  uint64_t article_id;
  uint8_t digest[SHA2_LENGTH];

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(article_id) + SHA2_LENGTH; }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.prefix_length);
    std::memcpy(out + pos, key.value, key.prefix_length);
    return pos + key.prefix_length;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.prefix_length);
    std::memcpy(key.value, in + pos, key.prefix_length);
    return pos + key.prefix_length;
  }

  static auto MaxFoldLength() -> uint32_t { return PrefixLength + sizeof(Key::prefix_length); }
};

using FileRelation      = benchmark::FileRelation<0, 1775872>;  // Max payload is retrieved from the traces
using InrowBlobRelation = leanstore::schema::InrowBlobRelation<0>;
using BlobPrefixIndex   = PrefixIndex<INDEX_PREFIX_LENGTH>;

}  // namespace wiki