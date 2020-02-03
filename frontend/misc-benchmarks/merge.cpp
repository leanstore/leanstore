#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/vs/BTreeSlotted.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = u64;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  // Check if parameters make sense
  // -------------------------------------------------------------------------------------
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  // LeanStore DB
  LeanStore db;
  unique_ptr<BTreeInterface<Key, Payload>> adapter;
  auto& vs_btree = db.registerVSBTree("contention");
  adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
  auto& table = *adapter;
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  Payload payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  const u64 tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));  // 2.0 corresponds to 50% space usage
  const u64 tuples_in_a_page = EFFECTIVE_PAGE_SIZE * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));
  // -------------------------------------------------------------------------------------
  PerfEvent e;
  // Insert values
  {
    const u64 n = tuple_count;
    {
      PerfEventBlock b(e, n);
      for (u64 t_i = 0; t_i < n; t_i++) {
        table.insert(t_i, payload);
      }
    }
    cout << "Inserted volume: (pages) = (" << db.getBufferManager().consumedPages() << ")" << endl;
  }
  // -------------------------------------------------------------------------------------
  u8 key_bytes[sizeof(Key)];
  BufferFrame* bf;
  for (u64 j = 0; j < FLAGS_x; j++)
    for (u64 i = 0; i < 1; i++) {
      u64 k = (tuples_in_a_page * j) + (10 + i);
      vs_btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16) { bf = &db.getBufferManager().getContainingBufferFrame(payload); });
      OptimisticGuard c_guard = OptimisticGuard(bf->header.lock);
      auto parent_handler = vs_btree.findParent(reinterpret_cast<void*>(&vs_btree), *bf);
      auto c_node = reinterpret_cast<leanstore::btree::vs::BTreeNode*>(bf->page.dt);
      u64 tuple_count = c_node->count;
      {
        u64 full_before = WorkerCounters::myCounters().dt_researchy[0][5], partial_before = WorkerCounters::myCounters().dt_researchy[0][6];
        PerfEventBlock b(e, 1);
        if (!vs_btree.checkSpaceUtilization(reinterpret_cast<void*>(&vs_btree), *bf, c_guard, parent_handler)) {
          b.print_in_destructor = false;
        } else {
          cout << tuple_count << '\t' << WorkerCounters::myCounters().dt_researchy[0][5] << '\t' << WorkerCounters::myCounters().dt_researchy[0][6]
               << endl;
        }
        // u64 full_diff = WorkerCounters::myCounters().dt_researchy[0][5] - full_before,
        //     partial_diff = WorkerCounters::myCounters().dt_researchy[0][6] - partial_before;
        // if (full_diff == 2 && partial_diff == 2)
        //   return 0;
      }
    }
  cout << "Inserted volume: (pages) = (" << db.getBufferManager().consumedPages() << ")" << endl;
  // -------------------------------------------------------------------------------------
  return 0;
}
