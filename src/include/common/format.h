#pragma once

#include "storage/extent/extent_list.h"
#include "storage/extent/large_page.h"

#include "fmt/format.h"

// NOLINTBEGIN

template <>
struct fmt::formatter<leanstore::storage::LargePage> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }

  template <typename Context>
  auto format(leanstore::storage::LargePage const &page, Context &ctx) const {
    return fmt::format_to(ctx.out(), "[{0}:{1}]", page.start_pid, page.page_cnt);
  }
};

template <>
struct fmt::formatter<leanstore::storage::ExtentTier> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }

  template <typename Context>
  auto format(leanstore::storage::ExtentTier const &page, Context &ctx) const {
    return fmt::format_to(ctx.out(), "[{0}:{1}]", page.start_pid, page.tier);
  }
};

// NOLINTEND