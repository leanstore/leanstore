#pragma once

#include "common/typedefs.h"

#include <algorithm>
#include <span>
#include <vector>

namespace leanstore::recovery {

/**
 * @brief Shrink every worker's prefix of commits to the largest prefixes which are closed under dependencies
 * `prefixes[w]` holds the commits of worker w in log order, `base_gsn(w)` is the stable GSN of w if its prefix is
 * empty. A dependency (w, gsn) is satisfied iff `gsn` <= the commit GSN of the last commit kept for w, because the
 * entries of a txn are stamped above the commit GSN of the txn before it. Each worker is cut before its first commit
 * with an unsatisfied dependency, until no cut moves. Since only the commits which have to go are removed, a cycle of
 * dependencies is kept as a whole.
 */
template <typename Commit, typename BaseGSN>
void CutToClosedPrefixes(std::span<std::vector<const Commit *>> prefixes, BaseGSN &&base_gsn) {
  auto stable_gsn = [&](wid_t w_i) -> timestamp_t {
    return prefixes[w_i].empty() ? base_gsn(w_i) : prefixes[w_i].back()->commit_gsn;
  };
  auto satisfied = [&](const Commit *commit) {
    return std::ranges::all_of(commit->dependencies,
                               [&](const auto &dep) { return dep.second <= stable_gsn(dep.first); });
  };
  for (auto changed = true; changed;) {
    changed = false;
    for (auto &commits : prefixes) {
      auto first_unsatisfied = std::ranges::find_if_not(commits, satisfied);
      if (first_unsatisfied != commits.end()) {
        commits.erase(first_unsatisfied, commits.end());
        changed = true;
      }
    }
  }
}

}  // namespace leanstore::recovery
