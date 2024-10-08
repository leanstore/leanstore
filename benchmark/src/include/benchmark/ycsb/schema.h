#pragma once

#include "leanstore/leanstore.h"

#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <cstring>

namespace ycsb {

// Supported sizes: 120 Bytes, 4KB, 100KB, 1MB, 10MB
static constexpr uint32_t SUPPORTED_PAYLOAD_SIZE[] = {120, 4096, 102400, 1048576, 10485760};
static constexpr uint32_t BLOB_NORMAL_PAYLOAD      = SUPPORTED_PAYLOAD_SIZE[0];
static constexpr uint32_t MAX_BLOB_REPRESENT_SIZE  = leanstore::BlobState::MAX_MALLOC_SIZE;

using YCSBKey = uint64_t;

template <typename TablePayload, int TypeIdValue>
struct Relation {
  static constexpr uint32_t TYPE_ID = TypeIdValue;

  struct Key {
    YCSBKey my_key;

    auto String() const -> std::string { return std::to_string(my_key); }

    void FromString(const std::string &s) { my_key = std::stoi(s); }
  };

  TablePayload payload;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(Relation<TablePayload, TypeIdValue>); }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.my_key);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.my_key);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::my_key); }
};

/**
 * @brief BlobState in relation format
 * Add a few utilities for Adapter to recognize this as a relation
 */
struct BlobStateRelation {
  static constexpr uint32_t TYPE_ID = 0;

  struct Key {
    YCSBKey my_key;
  };

  leanstore::BlobState payload;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return payload.MallocSize(); }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.my_key);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.my_key);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::my_key); }
};

}  // namespace ycsb