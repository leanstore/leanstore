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
/*
 plan:
1- Insert single-threaded and measure the cycles needed for a key with the worst filling grade.
2- Run a partial merge and full merge to measure the cycles needed to merge a key

0.55 + 0.55 >= 1 + 0.1 [cost of a partial merge] A: 776 cycles, instr: 2480 pro key
0.55 + 0.1 >= 0.65 [cost of a full merge] A: 38593 cycles:, instr: 45229.0 for 2x 0.433496 filled pages
291 -> 582
 */
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
  u64 size_at_insert_point;
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
    size_at_insert_point = db.getBufferManager().consumedPages();
    const u64 mib = size_at_insert_point * PAGE_SIZE / 1024 / 1024;
    cout << "Inserted volume: (pages, MiB) = (" << size_at_insert_point << ", " << mib << ")" << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  // -------------------------------------------------------------------------------------
  u8 key_bytes[sizeof(Key)];
  BufferFrame* bf;
  for (u64 j = 0; j < 2; j++)
    for (u64 i = 0; i < FLAGS_x; i++) {
      u64 k = tuples_in_a_page * (10 + i);
      vs_btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16) { bf = &db.getBufferManager().getContainingBufferFrame(payload); });
      auto c_node = reinterpret_cast<leanstore::btree::vs::BTreeNode*>(bf->page.dt);
      cout << c_node->fillFactor() << '\t' << c_node->count << endl;
      OptimisticGuard c_guard = OptimisticGuard(bf->header.lock);
      auto parent_handler = vs_btree.findParent(reinterpret_cast<void*>(&vs_btree), *bf);
      {
        PerfEventBlock b(e, 1);
        vs_btree.checkSpaceUtilization(reinterpret_cast<void*>(&vs_btree), *bf, c_guard, parent_handler);
      }
      cout << c_node->fillFactor() << '\t' << c_node->count << endl;
      cout << WorkerCounters::myCounters().dt_researchy[0][5] << '\t' << WorkerCounters::myCounters().dt_researchy[0][6] << endl;
    }
  // -------------------------------------------------------------------------------------
  return 0;
}
