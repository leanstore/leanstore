#pragma once

#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <string>

namespace benchmark {

struct RelationTest {
  static constexpr int TYPE_ID = 0;

  struct Key {
    Integer primary_id;

    auto String() const -> std::string { return std::to_string(primary_id); }

    void FromString(const std::string &s) { primary_id = std::stoi(s); }
  };

  Integer data;

  auto PayloadSize() const -> uint32_t { return sizeof(RelationTest); }

  static auto FoldKey(uint8_t *out, const RelationTest::Key &key) -> uint16_t {
    auto pos = Fold(out, key.primary_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, RelationTest::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.primary_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::primary_id); }
};

/**
 * @brief A sample of variable-size relation definition in LeanStore
 * The last attribute of such relations should be a flexible array member (FAM),
 *  which requires an attribute which expresses the size of this flexible array member
 * In the below example, the corresponding attributes are `uint8_t data[]` and `Integer size`, respectively
 *
 * To allocate such object, a char[] should be allocated first, either on stack or heap,
 *  then use placement new to allocate VariableSizeRelation() on that storage
 * Check `test/benchmark/test_leanstore_adapter.cc` for more information
 */
struct VariableSizeRelation {
  static constexpr int TYPE_ID = 1;

  struct Key {
    Integer primary_id;

    auto String() const -> std::string { return std::to_string(primary_id); }

    void FromString(const std::string &s) { primary_id = std::stoi(s); }
  };

  Integer size;
  uint8_t data[];

  explicit VariableSizeRelation(Integer size) : size(size) {}

  auto PayloadSize() const -> uint32_t { return sizeof(Integer) + size; }

  static auto FoldKey(uint8_t *out, const VariableSizeRelation::Key &key) -> uint16_t {
    auto pos = Fold(out, key.primary_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, VariableSizeRelation::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.primary_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::primary_id); }
};

}  // namespace benchmark