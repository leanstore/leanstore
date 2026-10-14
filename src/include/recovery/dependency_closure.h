#pragma once

#include "common/typedefs.h"

#include <algorithm>
#include <cstddef>
#include <span>
#include <vector>

namespace leanstore::recovery {

/**
 * @brief Cut every worker's durable log to the largest prefixes which are closed under dependencies
 * `dependent[w]` holds the durable commits of worker w which have dependencies, in log order. Only these can end a
 * prefix. The returned cut[w] is the index of the first of them outside w's prefix, which then ends at that commit's
 * `prev_commit_gsn`, or at `durable_gsn[w]` if cut[w] == dependent[w].size(). A dependency (w, gsn) is satisfied iff
 * `gsn` <= the end of w's prefix, because the entries of a txn are stamped above the commit GSN of the txn before it.
 * Only the commits which have to go are cut, so a cycle of dependencies is kept as a whole.
 */
template <typename Commit>
auto CutToClosedPrefixes(std::span<const std::vector<const Commit *>> dependent,
                         std::span<const timestamp_t> durable_gsn) -> std::vector<std::size_t> {
  auto cut = std::vector<std::size_t>(dependent.size());
  for (std::size_t w_i = 0; w_i < dependent.size(); w_i++) { cut[w_i] = dependent[w_i].size(); }

  auto prefix_gsn = [&](wid_t w_i) -> timestamp_t {
    return cut[w_i] == dependent[w_i].size() ? durable_gsn[w_i] : dependent[w_i][cut[w_i]]->prev_commit_gsn;
  };
  auto satisfied = [&](const Commit *commit) {
    return std::ranges::all_of(commit->dependencies,
                               [&](const auto &dep) { return dep.second <= prefix_gsn(dep.first); });
  };
  for (auto changed = true; changed;) {
    changed = false;
    for (std::size_t w_i = 0; w_i < dependent.size(); w_i++) {
      auto kept              = std::span{dependent[w_i]}.first(cut[w_i]);
      auto first_unsatisfied = std::ranges::find_if_not(kept, satisfied);
      if (first_unsatisfied != kept.end()) {
        cut[w_i] = first_unsatisfied - kept.begin();
        changed  = true;
      }
    }
  }
  return cut;
}

}  // namespace leanstore::recovery
