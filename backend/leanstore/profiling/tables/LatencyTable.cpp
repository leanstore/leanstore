#include "LatencyTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
std::string LatencyTable::getName()
{
   return "latency";
}
// -------------------------------------------------------------------------------------
void LatencyTable::open()
{
   columns.emplace("key", [](Column&) {});
   columns.emplace("tx_i", [](Column&) {});
   columns.emplace("cc_ms_precommit_latency", [](Column&) {});
   columns.emplace("cc_ms_commit_latency", [](Column&) {});
   columns.emplace("cc_flushes_counter", [](Column&) {});
   columns.emplace("cc_rfa_ms_precommit_latency", [](Column&) {});
   columns.emplace("cc_rfa_ms_commit_latency", [](Column&) {});
}
// -------------------------------------------------------------------------------------
void LatencyTable::next()
{
   clear();
   // -------------------------------------------------------------------------------------
   for (auto w_i = CRCounters::cr_counters.begin(); w_i != CRCounters::cr_counters.end(); ++w_i) {
      if (w_i->worker_id.load() != -1) {
         continue;
      }
      for (u64 tx_i = 0; tx_i < CRCounters::latency_tx_capacity; tx_i++) {
         columns.at("key") << w_i->worker_id.load();
         columns.at("tx_i") << tx_i;
         columns.at("cc_ms_precommit_latency") << w_i->cc_ms_precommit_latency[tx_i].load();
         columns.at("cc_ms_commit_latency") << w_i->cc_ms_commit_latency[tx_i].load();
         columns.at("cc_flushes_counter") << w_i->cc_flushes_counter[tx_i].load();
         columns.at("cc_rfa_ms_precommit_latency") << w_i->cc_rfa_ms_precommit_latency[tx_i].load();
         columns.at("cc_rfa_ms_commit_latency") << w_i->cc_rfa_ms_commit_latency[tx_i].load();
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
