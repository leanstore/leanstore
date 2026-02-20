#pragma once

#include "common/typedefs.h"
#include "transaction/svcc/wait_die_lock.h"

#include "tbb/concurrent_hash_map.h"

#include <cstring>
#include <map>
#include <set>
#include <stdexcept>
#include <unordered_map>

namespace leanstore::transaction::svcc {

struct LockableTuple;

struct RawDelete {
  void operator()(LockableTuple *p) const { ::operator delete(p); }
};

using LockableTupleAsKey = std::unique_ptr<LockableTuple, RawDelete>;

struct LockableTuple {
  u64 tree_id;  // B-tree identifier
  u8 key_len;
  u8 key[];  // Key as byte array

  static auto Constructor(std::span<u8> key_span, u64 tree_id) -> LockableTupleAsKey {
    size_t total_size = sizeof(LockableTuple) + key_span.size() * sizeof(u8);

    // Allocate memory
    auto ptr = (LockableTuple *)::operator new(total_size);

    // Initialize fields
    ptr->tree_id = tree_id;
    ptr->key_len = static_cast<u8>(key_span.size());
    std::memcpy(ptr->key, key_span.data(), key_span.size());

    // Wrap in unique_ptr with custom deleter
    return LockableTupleAsKey(ptr);
  }

  struct HashCompare {
    auto hash(const LockableTupleAsKey &ptr) const {
      size_t hash = std::hash<uint64_t>{}(ptr->tree_id);
      for (auto idx = 0U; idx < ptr->key_len; idx++) {
        hash ^= std::hash<uint8_t>{}(ptr->key[idx]) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
      }
      return hash;
    }

    auto equal(const LockableTupleAsKey &a, const LockableTupleAsKey &b) const {
      return a->tree_id == b->tree_id && a->key_len == b->key_len && memcmp(a->key, b->key, a->key_len) == 0;
    }
  };
};

class LockManager {
 public:
  LockManager()  = default;
  ~LockManager() = default;

 private:
  tbb::concurrent_hash_map<LockableTupleAsKey, WaitDieLock, LockableTuple::HashCompare> internal_;
};

}  // namespace leanstore::transaction::svcc
