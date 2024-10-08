#pragma once

#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "storage/extent/large_page.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iterator>
#include <limits>
#include <ranges>
#include <vector>

namespace leanstore::storage {

/**
 * @brief This is different from `struct LargePage`
 * LargePage is supposed to represent general-purpose LargePage with arbitrary size (page count) within LeanStore
 * On the other hand, TailExtent's purpose is to provide better storage utilization for static Blob
 *
 * Because of that, Tail Extent is unnecessarily bigger than ExtentList::MAX_EXTENT_SIZE
 */
struct TailExtent {
  pageid_t start_pid;
  u64 page_cnt : 48;
} __attribute__((packed));

/**
 * @brief Every BlobState refers to a list of large pages/extents (similar to `extent` in filesystem)
 * When a new Blob (and the representative BlobState) is allocated, its new content must be written somewhere
 * There are two possible solutions:
 * - Allocate a new large page only to store the newly-created content
 * - Append the content to an existing extent, which should be big enough to store existing content + new one
 * In the first approach, the number of extents a Stem Blob can allocate is unbounded,
 *  thus storing extents in BlobState is infeasible as its size is unlimitted and not fit in a single B-Tree page
 * In other words, first approach requires a B-Tree to manage all actives BlobStates in our system,
 *  which makes Blob allocation/garbage collection complicated
 * Therefore, LeanStore advocates for the 2nd solution.
 */
struct ExtentList {
  // -------------------------------------------------------------------------------------
  static constexpr auto EXTENT_PER_LEVEL    = 10UL;
  static constexpr auto NUMBER_OF_LEVELS    = 12UL;
  static constexpr extidx_t NO_TIERS        = NUMBER_OF_LEVELS * EXTENT_PER_LEVEL;
  static constexpr extidx_t EXTENT_CNT_MASK = 0x7F;  // also means the max # extents. 2^7 - 1 = 127 extents

  // Pre-computed size of all tiers, unit is DB page
  static constexpr auto TIER_SIZE{[]() constexpr {
    std::array<u64, NO_TIERS> result{};
    for (auto level_i = 0UL; level_i < NUMBER_OF_LEVELS; ++level_i) {
      result[level_i * EXTENT_PER_LEVEL] = Power(level_i + 1, EXTENT_PER_LEVEL);
      for (auto i = 1UL; i < EXTENT_PER_LEVEL; ++i) {
        auto idx    = level_i * EXTENT_PER_LEVEL + i;
        result[idx] = (level_i + 2) * result[idx - 1] / (level_i + 1);
      }
    }
    return result;
  }()};
  static constexpr auto TOTAL_TIER_SIZE{[]() constexpr {
    std::array<u64, NO_TIERS> result{};
    result[0] = TIER_SIZE[0];
    for (int i = 1; i < NO_TIERS; ++i) { result[i] = result[i - 1] + TIER_SIZE[i]; }
    return result;
  }()};
  static constexpr u64 MAX_EXTENT_SIZE = NUMBER_OF_LEVELS * (Power(NUMBER_OF_LEVELS + 1, EXTENT_PER_LEVEL - 1));

  // Total size of the first NUMBER_OF_LEVELS * EXTENT_PER_LEVEL extents
  static constexpr u64 INITIAL_SIZE = ArraySum<u64, NO_TIERS>(TIER_SIZE);

  // -------------------------------------------------------------------------------------
  /* All attributes */
  TailExtent tail;
  bool tail_in_used : 1;    // Whether the tail extent is being used
  extidx_t extent_cnt : 7;  // The number of extents of this Blob, excluding tail extent
  pageid_t extent_pid[];    // The start PID of every extents

  // -------------------------------------------------------------------------------------
  /**
   * @brief Useful Iterator and operator utilities for accessing the extents
   */
  struct Iterator {
    explicit Iterator(const ExtentList *ext, extidx_t pos = 0) : pos_(pos), data_(0, 0), parent_(ext) {
      RetrieveData();
    }

    ~Iterator() = default;

    auto operator*() const -> const LargePage & { return data_; }

    auto operator++() -> Iterator & {
      pos_++;
      RetrieveData();
      return *this;
    }

    friend auto operator!=(const Iterator &a, const Iterator &b) -> bool {
      return (a.parent_ != b.parent_) || (a.pos_ != b.pos_);
    };

   private:
    extidx_t pos_;
    LargePage data_;
    const ExtentList *parent_;

    void RetrieveData() {
      if (pos_ < parent_->NumberOfExtents()) {
        pageid_t pid = parent_->extent_pid[pos_];
        data_        = LargePage(pid, ExtentList::ExtentSize(pos_));
      }
    }
  };

  // NOLINTBEGIN
  auto begin() const -> Iterator { return Iterator(this, 0); }

  auto end() const -> Iterator { return Iterator(this, NumberOfExtents()); }

  // NOLINTEND

  auto operator[](int idx) const -> LargePage {
    assert(idx < NumberOfExtents());
    return {extent_pid[idx], ExtentSize(idx)};
  }

  // -------------------------------------------------------------------------------------
  /**
   * @brief Number of pages that extent `extent_id` have
   */
  static auto ExtentSize(extidx_t extent_id) -> u64 {
    return (extent_id < NO_TIERS) ? TIER_SIZE[extent_id] : MAX_EXTENT_SIZE;
  }

  /**
   * @brief Total size of all extents of range [0..idx]
   * The size unit is DB pages
   */
  static auto TotalSizeExtents(extidx_t idx) -> u64 {
    if (idx < NO_TIERS) { return TOTAL_TIER_SIZE[idx]; }
    return INITIAL_SIZE + (idx - NO_TIERS + 1) * MAX_EXTENT_SIZE;
  }

  /**
   * @brief Size of an ExtentList with specified number of extents
   */
  static constexpr auto MallocSize(extidx_t no_extents) -> u64 {
    return sizeof(ExtentList) + sizeof(pageid_t) * no_extents;
  }

  /**
   * @brief Find one tier that fits page_cnt, i.e. find idx such that TIER_SIZE[idx] == page_cnt
   */
  static auto TierIndex(u64 page_cnt) -> extidx_t {
    auto index = std::ranges::lower_bound(TIER_SIZE, page_cnt);
    Ensure(index != TIER_SIZE.end());
    return index - TIER_SIZE.begin();
  }

  /**
   * @brief Number of extents (from tier 0) that a payload of `size` pages spans across on
   */
  static auto NoSpanExtents(pageid_t no_pages) -> extidx_t {
    uint64_t no_extents;
    if (no_pages <= INITIAL_SIZE) [[likely]] {
      no_extents = std::ranges::find_if_not(TOTAL_TIER_SIZE, [&no_pages](u64 other) { return other < no_pages; }) -
                   TOTAL_TIER_SIZE.begin() + 1;
    } else {
      no_extents = NO_TIERS + std::ceil(static_cast<double>(no_pages - INITIAL_SIZE) / MAX_EXTENT_SIZE);
    }
    return no_extents;
  };

  // -------------------------------------------------------------------------------------
  /**
   * @brief Number of extents, i.e. size of `extent_pid` attribute
   */
  auto NumberOfExtents() const -> extidx_t { return extent_cnt; }

  /**
   * @brief Whether all the extents referred by `extent_pid` are contiguous
   */
  auto TotalSizeExtents() const -> u64 { return TotalSizeExtents(NumberOfExtents() - 1); }
} __attribute__((packed));

// Magic numbers
static_assert(ExtentList::TIER_SIZE[ExtentList::NO_TIERS - 1] == ExtentList::MAX_EXTENT_SIZE);
static_assert(ExtentList::MAX_EXTENT_SIZE < (1UL << 48) - 1);
static_assert(sizeof(TailExtent) == 14);
static_assert(sizeof(ExtentList) == 15);
static_assert(ExtentList::MallocSize(ExtentList::EXTENT_CNT_MASK) == 1031);

}  // namespace leanstore::storage