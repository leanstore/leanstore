#pragma once

#include "fmt/core.h"
#include "share_headers/db_types.h"
#include "typefold/typefold.h"

namespace benchmark {

template <int TypeID, uint32_t MaxPayload>
struct FileRelation {
  static constexpr int TYPE_ID = TypeID;

  struct Key {
    uint64_t my_key;

    auto String() const -> std::string { return fmt::format("{}_{}", TypeID, my_key); }

    void FromString(const std::string &suffix) { my_key = std::stoi(suffix.substr(suffix.find('_'), suffix.size())); }
  };

  Varchar<MaxPayload> payload;

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

}  // namespace benchmark
