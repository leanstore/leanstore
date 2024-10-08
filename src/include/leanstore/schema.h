#pragma once

#include "storage/blob/blob_state.h"
#include "storage/btree/node.h"

#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <cstring>

namespace leanstore::schema {

struct KeyValueRelation {
  static constexpr u32 TYPE_ID = ~0;

  struct Key {
    uint64_t key;
  };

  uint64_t value;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(uint64_t); }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.key);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.key);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::key); }
};

template <int TypeID>
struct BlobRelation {
  static constexpr u32 TYPE_ID = TypeID;

  struct Key {
    uint64_t my_key;
  };

  uint8_t payload[storage::blob::BlobState::MIN_MALLOC_SIZE];

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t {
    return reinterpret_cast<const storage::blob::BlobState *>(payload)->MallocSize();
  }

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
 * @brief Maximum in-row (in-page) payload size for InrowBlobRelation relation
 *
 * Calculated by:
 * storage::BTreeNode::MAX_RECORD_SIZE: maximum tuple size in a BTree page
 * sizeof(uint16_t): size of InrowBlobRelation::inrow_size property
 * sizeof(uint64_t): size of InrowBlobRelation::Key
 */
static constexpr uint32_t MAX_INROW_SIZE = storage::BTreeNode::MAX_RECORD_SIZE - sizeof(uint16_t) - sizeof(uint64_t);

template <int TypeID>
struct InrowBlobRelation {
  static constexpr u32 TYPE_ID = TypeID;

  struct Key {
    uint64_t my_key;
  };

  /** inrow_size == 0 means BlobState, else size of in-row data */
  uint16_t inrow_size;
  uint8_t payload[];  // Maximum size of payload should be MAX_INROW_SIZE bytes

  InrowBlobRelation(uint64_t inrow_size, std::span<const uint8_t> s) : inrow_size(inrow_size) {
    if (inrow_size > 0) { Ensure(inrow_size == s.size()); }
    std::memcpy(payload, s.data(), s.size());
  }

  // -------------------------------------------------------------------------------------
  static constexpr size_t MAX_RECORD_SIZE = MAX_INROW_SIZE + sizeof(inrow_size);

  auto IsOffrowRecord() const -> bool { return inrow_size == 0; }

  auto PayloadSize() const -> uint32_t {
    auto ret = sizeof(inrow_size);
    if (inrow_size == 0) {
      ret += reinterpret_cast<const storage::blob::BlobState *>(payload)->MallocSize();
    } else {
      ret += inrow_size;
    }
    return ret;
  }

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

static_assert(MAX_INROW_SIZE >= storage::blob::BlobState::MAX_MALLOC_SIZE);

}  // namespace leanstore::schema