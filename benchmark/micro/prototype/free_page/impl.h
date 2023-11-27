#pragma once

#include "fmt/ranges.h"
#include "gtest/gtest_prod.h"

#include <atomic>
#include <bitset>
#include <cassert>
#include <climits>
#include <cmath>
#include <list>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#define TIER_SIZE(tier_id) (1 << (tier_id))

/**
 * @brief All following impls power-of-2 tiering
 * Real algorithm should find a better distribution which suits the workload
 */
struct FreePageManager {
  FreePageManager(int number_of_pages) : no_pages(number_of_pages), alloc_cnt{0}, physical_cnt{0} {}

  virtual constexpr auto SystemName() -> const char           * = 0;
  virtual auto AllocTierPage(int tier, size_t &out_pid) -> bool = 0;
  virtual void DeallocTierPage(size_t page_id, int tier)        = 0;

  size_t no_pages;
  std::atomic<size_t> alloc_cnt;
  std::atomic<size_t> physical_cnt;
};

struct FirstFitAllocator : FreePageManager {
  FirstFitAllocator(int number_of_pages) : FreePageManager(number_of_pages) {}

  constexpr auto SystemName() -> const char * override { return "firstfit"; }

  auto AllocTierPage(int tier, size_t &out_pid) -> bool {
    size_t required_pages = TIER_SIZE(tier);

    for (auto ite = free_pages.begin(); ite != free_pages.end(); ++ite) {
      auto [start_pid, size] = *ite;
      if (size >= required_pages) {
        out_pid = start_pid;
        free_pages.erase(ite);
        physical_cnt += required_pages;

        if (size > required_pages) { free_pages.emplace_back(out_pid + required_pages, size - required_pages); }
        return true;
      }
    }

    if (alloc_cnt + required_pages <= no_pages) {
      out_pid = alloc_cnt;
      alloc_cnt += required_pages;
      physical_cnt += required_pages;
      return true;
    }

    return false;
  }

  void DeallocTierPage(size_t page_id, int tier) {
    size_t required_pages = TIER_SIZE(tier);
    physical_cnt -= required_pages;

    free_pages.emplace_back(page_id, required_pages);
  }

  std::list<std::pair<size_t, size_t>> free_pages;  // Linked-list of free pages <start PID, size>
};

struct BitmapFreePages : FreePageManager {
  BitmapFreePages(int number_of_pages) : FreePageManager(number_of_pages), page_counter(0) {
    bitmap = std::vector<std::bitset<64>>(std::ceil(static_cast<double>(no_pages) / 64), 0);
  }

  constexpr auto SystemName() -> const char * override { return "bitmap"; }

  auto AllocTierPage(int tier, size_t &out_pid) -> bool override {
    size_t required_pages = TIER_SIZE(tier);

    auto loop_over_end     = false;
    size_t initial_counter = page_counter;

    for (; (!loop_over_end) || (page_counter < initial_counter);) {
      if (page_counter >= no_pages) {
        loop_over_end = true;
        page_counter  = 0;
      }
      page_counter = page_counter % no_pages;

      for (; page_counter < no_pages; page_counter++) {
        auto block = page_counter / 64;

        if (bitmap[block].count() == 0) {
          page_counter = ((page_counter + 64) / 64) * 64;
          continue;
        }

        if (bitmap[block].test(page_counter % 64)) {
          auto j = page_counter + 1;
          for (; (j < no_pages) && (j - page_counter < required_pages); j++) {
            auto j_block = j / 64;
            if (bitmap[j_block].count() == 64) {
              j = ((j + 64) / 64) * 64;
              continue;
            }
            if (!bitmap[j_block].test(j % 64)) { break; }
          }
          //  Found a free page range
          if (j - page_counter >= required_pages) {
            for (auto k = page_counter; k < page_counter + required_pages; k++) {
              auto k_block = k / 64;
              bitmap[k_block].reset(k % 64);
            }
            out_pid      = page_counter;
            page_counter = out_pid + required_pages;
            physical_cnt += required_pages;
            return true;
          }
          page_counter = j - 1;
        }
      }
    }

    if (alloc_cnt + required_pages <= no_pages) {
      out_pid = alloc_cnt;
      alloc_cnt += required_pages;
      physical_cnt += required_pages;
      return true;
    }

    return false;
  }

  void DeallocTierPage(size_t page_id, int tier) {
    size_t required_pages = TIER_SIZE(tier);
    for (auto k = page_id; k < page_id + required_pages; k++) {
      auto k_block = k / 64;
      bitmap[k_block].set(k % 64);
    }
    physical_cnt -= required_pages;
  }

  size_t page_counter;
  std::vector<std::bitset<64>> bitmap;
};

struct TreeFreePages : FreePageManager {
  TreeFreePages(int number_of_pages) : FreePageManager(number_of_pages) {}

  constexpr auto SystemName() -> const char * override { return "tree"; }

  void InsertRange(size_t start_pid, size_t size) {
    auto end_pid = start_pid + size - 1;
    free_pages.emplace(size, start_pid);
    free_start_pid.emplace(start_pid, size);
    free_end_pid.emplace(end_pid, size);
  }

  void RemoveRange(size_t start_pid, size_t size) {
    auto end_pid = start_pid + size - 1;
    free_pages.erase(std::make_pair(size, start_pid));
    free_start_pid.erase(start_pid);
    free_end_pid.erase(end_pid);
  }

  auto AllocTierPage(int tier, size_t &out_pid) -> bool override {
    size_t required_pages = TIER_SIZE(tier);

    auto lb = free_pages.lower_bound(std::make_pair(required_pages, 0));
    if (lb != free_pages.end()) {
      size_t free_size;
      std::tie(free_size, out_pid) = *lb;
      RemoveRange(out_pid, free_size);

      if (free_size > required_pages) { InsertRange(out_pid + required_pages, free_size - required_pages); }
      physical_cnt += required_pages;
      return true;
    }

    if (alloc_cnt + required_pages <= no_pages) {
      out_pid = alloc_cnt;
      alloc_cnt += required_pages;
      physical_cnt += required_pages;
      return true;
    }

    return false;
  }

  void DeallocTierPage(size_t page_id, int tier) {
    size_t required_pages = TIER_SIZE(tier);
    physical_cnt -= required_pages;

    if (page_id > 0 && free_end_pid.contains(page_id - 1)) {
      size_t unused, size;
      std::tie(unused, size) = *(free_end_pid.find(page_id - 1));
      auto start_pid         = (page_id - 1) - size + 1;
      RemoveRange(start_pid, size);
      page_id = start_pid;
      required_pages += size;
    }

    if (free_start_pid.contains(page_id + required_pages)) {
      size_t unused, size;
      std::tie(unused, size) = *(free_start_pid.find(page_id + required_pages));
      RemoveRange(page_id + required_pages, size);
      required_pages += size;
    }

    InsertRange(page_id, required_pages);
  }

  std::set<std::pair<size_t, size_t>> free_pages;     // Ordered map of <free page size, start_pid>
  std::unordered_map<size_t, size_t> free_start_pid;  // Free range: Start PID -> Size
  std::unordered_map<size_t, size_t> free_end_pid;    // Free range: End PID -> Size
};

template <int HighestTier>
struct TieringFreePages : FreePageManager {
  TieringFreePages(int number_of_pages) : FreePageManager(number_of_pages) {}

  constexpr auto SystemName() -> const char * override { return "tiering"; }

  auto AllocTierPage(int tier, size_t &out_pid) -> bool override {
    if (free_pages[tier].size() > 0) {
      out_pid = free_pages[tier].back();
      free_pages[tier].pop_back();
      physical_cnt += TIER_SIZE(tier);
      return true;
    }

    for (int i = HighestTier - 1; i >= tier; i--) {
      auto x = (i == tier) ? HighestTier : i;
      if (free_pages[x].size() > 0) {
        out_pid = free_pages[x].back();
        free_pages[x].pop_back();
        auto offset = TIER_SIZE(tier);
        for (auto j = tier; j < x; j++) {
          free_pages[j].push_back(out_pid + offset);
          offset += TIER_SIZE(j);
        }
        physical_cnt += TIER_SIZE(tier);
        return true;
      }
    }

    if (alloc_cnt + TIER_SIZE(tier) <= no_pages) {
      out_pid = alloc_cnt;
      alloc_cnt += TIER_SIZE(tier);
      physical_cnt += TIER_SIZE(tier);
      return true;
    }

    return false;
  }

  void DeallocTierPage(size_t page_id, int tier) {
    free_pages[tier].emplace_back(page_id);
    physical_cnt -= TIER_SIZE(tier);
  }

  std::array<std::vector<size_t>, HighestTier + 1> free_pages;
};
