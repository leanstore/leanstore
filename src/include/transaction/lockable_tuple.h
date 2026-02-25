#pragma once

#include "common/typedefs.h"

#include <alloca.h>
#include <cstddef>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <stdexcept>
#include <unordered_map>

namespace leanstore::transaction {

static constexpr timestamp_t INVALID_TS = std::numeric_limits<timestamp_t>::max();

struct LockableTuple {
  leng_t tree_id;  // B-tree identifier
  u8 key_len;
  u8 key[];  // Key as byte array

  LockableTuple(std::span<u8> key_span, u64 tree_id) = delete;

  static auto Constructor(std::span<u8> key_span, u64 tree_id) -> LockableTuple * {
    size_t total_size = sizeof(LockableTuple) + key_span.size() * sizeof(u8);

    // Allocate memory
    auto ptr = (LockableTuple *)::operator new(total_size);

    // Initialize fields
    ptr->tree_id = tree_id;
    ptr->key_len = static_cast<u8>(key_span.size());
    std::memcpy(ptr->key, key_span.data(), key_span.size());

    // Wrap in unique_ptr with custom deleter
    return ptr;
  }

  // Copy constructor (allocates a new object on the heap)
  static auto Constructor(const LockableTuple &other) -> LockableTuple * {
    size_t total_size = sizeof(LockableTuple) + other.key_len * sizeof(u8);
    auto ptr          = (LockableTuple *)::operator new(total_size);

    // Copy fields
    ptr->tree_id = other.tree_id;
    ptr->key_len = other.key_len;
    std::memcpy(ptr->key, other.key, other.key_len);
    return ptr;
  }

  static void Release(LockableTuple *ptr) { ::operator delete(ptr); }

  static inline auto HashCore(const LockableTuple *ptr) -> size_t {
    size_t h = std::hash<uint64_t>{}(ptr->tree_id);
    for (size_t i = 0; i < ptr->key_len; i++) {
      h ^= std::hash<uint8_t>{}(ptr->key[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
  }

  static inline bool EqualCore(const LockableTuple *a, const LockableTuple *b) {
    return a->tree_id == b->tree_id && a->key_len == b->key_len && std::memcmp(a->key, b->key, a->key_len) == 0;
  }

  struct HashTBB {
    size_t hash(const LockableTuple *ptr) const { return LockableTuple::HashCore(ptr); }

    bool equal(const LockableTuple *a, const LockableTuple *b) const { return LockableTuple::EqualCore(a, b); }
  };

  struct HashPtr {
    size_t operator()(const LockableTuple *ptr) const { return LockableTuple::HashCore(ptr); }
  };

  struct EqualPtr {
    bool operator()(const LockableTuple *a, const LockableTuple *b) const { return LockableTuple::EqualCore(a, b); }
  };
};

#define LOCKABLE_TUPLE_STACK(name, key_span, tree_id_val)                                               \
  size_t name##_total_size = sizeof(transaction::LockableTuple) + (key_span).size();                    \
  auto name                = reinterpret_cast<transaction::LockableTuple *>(alloca(name##_total_size)); \
  name->tree_id            = (tree_id_val);                                                             \
  name->key_len            = static_cast<u8>((key_span).size());                                        \
  std::memcpy(name->key, (key_span).data(), (key_span).size());

}  // namespace leanstore::transaction
