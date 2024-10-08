#pragma once

#include "leanstore/leanstore.h"

#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <cstring>

using FilePath = Varchar<128>;

namespace leanstore::fuse {

struct FileRelation {
  static constexpr int TYPE_ID = 0;

  struct Key {
    FilePath file_name;
  };

  leanstore::BlobState file_meta;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return file_meta.MallocSize(); }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.file_name);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.file_name);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::file_name); }
};

}  // namespace leanstore::fuse