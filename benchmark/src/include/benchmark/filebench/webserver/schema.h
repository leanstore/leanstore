#pragma once

#include "share_headers/db_types.h"
#include "typefold/typefold.h"

namespace filebench::webserver {

static constexpr uint32_t MAX_LOG_ENTRY_SIZE = 32 * 1024;  // 32 KB
static constexpr uint32_t MEAN_FILE_SIZE     = 16 * 1024;  // 16 KB
static constexpr uint32_t MIN_FILE_SIZE      = 4096;
static constexpr uint32_t MAX_FILE_SIZE      = 102400;
static constexpr double GAMMA_DIST_GAMMA     = 1.5;
static constexpr uint32_t MAX_BLOB_REP_SIZE  = leanstore::BlobState::MAX_MALLOC_SIZE;

/**
 * @brief BlobState in relation format
 * Add a few utilities for Adapter to recognize this as a relation
 */
template <int TypeID>
struct BlobStateRelation {
  static constexpr uint32_t TYPE_ID = TypeID;

  struct Key {
    Integer my_key;
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

}  // namespace filebench::webserver