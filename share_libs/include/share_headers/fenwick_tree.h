#pragma once

#include <vector>

/**
 * @brief Fenwick Tree implementation - used for Range Sum query
 */
class SumFenwickTree {
 public:
  void Construct(uint32_t size) {
    internal_.resize(size, 0);
  }

  void Insert(uint32_t idx, int64_t val) {
    while (idx < internal_.size()) {
      internal_[idx] += val;
      idx += idx & -idx;
    }
  }

  auto Query(uint32_t idx) -> int64_t {
    int64_t result = 0;
    while (idx != 0) {
      result += internal_[idx];
      idx = idx & (idx - 1);
    }
    return result;
  }

  /* Range query, inclusive both ends */
  auto RangeQuery(uint32_t from, uint32_t to) {
    if (from == 0) { return Query(to); }
    return Query(to) - Query(from - 1);
  }

 private:
  std::vector<int64_t> internal_;
};
