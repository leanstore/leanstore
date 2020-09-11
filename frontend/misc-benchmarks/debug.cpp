#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/BTreeSlotted.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_bool(only_warehouse, false, "");
DEFINE_uint64(matrix_mul, 0, "");
DEFINE_uint64(pread_pct, 0, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = BytesPayload<120>;
// -------------------------------------------------------------------------------------
struct alignas(64) BF {
  HybridLatch latch;
};
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  // Check if parameters make sense
  // -------------------------------------------------------------------------------------
  // LeanStore DB
  LeanStore db;
  db.registerConfigEntry("only_warehouse", [&](ostream& out) { out << FLAGS_only_warehouse; });
  db.registerConfigEntry("pread_pct", [&](ostream& out) { out << FLAGS_pread_pct; });
  // -------------------------------------------------------------------------------------
  unique_ptr<BTreeInterface<Key, Payload>> warehouse_adapter;
  auto& warehouse_vs_btree = db.registerVSBTree("warehouse");
  warehouse_adapter.reset(new BTreeVSAdapter<Key, Payload>(warehouse_vs_btree));
  auto& warehouse_table = *warehouse_adapter;

  unique_ptr<BTreeInterface<Key, Payload>> order_adapter;
  auto& order_vs_btree = db.registerVSBTree("order");
  order_adapter.reset(new BTreeVSAdapter<Key, Payload>(order_vs_btree));
  auto& order_table = *order_adapter;

  unique_ptr<BTreeInterface<Key, Payload>> order_status_adapter;
  auto& order_status_vs_btree = db.registerVSBTree("order_status");
  order_status_adapter.reset(new BTreeVSAdapter<Key, Payload>(order_status_vs_btree));
  auto& order_status_table = *order_status_adapter;

  unique_ptr<BTreeInterface<Key, Payload>> delivery_adapter;
  auto& delivery_vs_btree = db.registerVSBTree("delivery");
  delivery_adapter.reset(new BTreeVSAdapter<Key, Payload>(delivery_vs_btree));
  auto& delivery_table = *delivery_adapter;

  unique_ptr<BTreeInterface<Key, Payload>> stock_adapter;
  auto& stock_vs_btree = db.registerVSBTree("stock");
  stock_adapter.reset(new BTreeVSAdapter<Key, Payload>(stock_vs_btree));
  auto& stock_table = *stock_adapter;

  unique_ptr<BTreeInterface<Key, Payload>> new_order_adapter;
  auto& new_order_vs_btree = db.registerVSBTree("new_order");
  new_order_adapter.reset(new BTreeVSAdapter<Key, Payload>(new_order_vs_btree));
  auto& new_order_table = *new_order_adapter;
  // -------------------------------------------------------------------------------------
  Payload payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  u64 tuple_count;
  tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(Key) + sizeof(Payload));
  // -------------------------------------------------------------------------------------
  const u64 distance = 1000;
  const u64 fill_threads = 256;
  for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++)
    warehouse_table.insert(t_i, payload);
  if (!FLAGS_only_warehouse)
    for (u64 t_i = 0; t_i < distance * fill_threads; t_i++) {
      order_table.insert(t_i, payload);
      order_status_table.insert(t_i, payload);
      delivery_table.insert(t_i, payload);
      stock_table.insert(t_i, payload);
      new_order_table.insert(t_i, payload);
    }
  // -------------------------------------------------------------------------------------
  cout << "Inserts done, warehouse pages = " << warehouse_vs_btree.countPages() << endl;
  // -------------------------------------------------------------------------------------
  db.startDebuggingThread();
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  atomic<bool> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  vector<thread> threads;
  // -------------------------------------------------------------------------------------
  if (FLAGS_matrix_mul) {
    threads.emplace_back([&]() {
      running_threads_counter++;
      const int matrix_size = FLAGS_matrix_mul;
      float **A, **B, **C;

      A = new float*[matrix_size];
      B = new float*[matrix_size];
      C = new float*[matrix_size];

      for (int i = 0; i < matrix_size; i++) {
        A[i] = new float[matrix_size];
        B[i] = new float[matrix_size];
        C[i] = new float[matrix_size];
      }

      for (int i = 0; i < matrix_size; i++) {
        for (int j = 0; j < matrix_size; j++) {
          A[i][j] = rand();
          B[i][j] = rand();
        }
      }
      while (keep_running) {
        for (int i = 0; i < matrix_size; i++) {
          for (int j = 0; j < matrix_size; j++) {
            C[i][j] = 0;
            for (int k = 0; k < matrix_size; k++) {
              C[i][j] += A[i][k] * B[k][j];
            }
            WorkerCounters::myCounters().tx++;
          }
        }
      }
      for (int i = 0; i < matrix_size; i++) {
        delete A[i];
        delete B[i];
        delete C[i];
      }

      delete A;
      delete B;
      delete C;
      running_threads_counter--;
    });
  }
  // -------------------------------------------------------------------------------------
  leanstore::buffermanager::BufferFrame dump;
  if (FLAGS_worker_threads != 999)
    for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back(
          [&](u64 t_i) {
            running_threads_counter++;
            pthread_setname_np(pthread_self(), "worker");
            if (FLAGS_pin_threads)
              utils::pinThisThreadRome(FLAGS_pp_threads + t_i);
            while (keep_running) {
              if (FLAGS_pread_pct) {
                u64 rnd = leanstore::utils::RandomGenerator::getRandU64(0, 1000);
                if (rnd < FLAGS_pread_pct) {
                  db.getBufferManager().readPageSync(0, dump.page);
                  goto end;
                }
              }
              if (FLAGS_only_warehouse) {
                warehouse_table.update(t_i, payload);
                goto end;
              } else {
                int rnd = leanstore::utils::RandomGenerator::getRand(0, 1000);
                if (rnd < 430) {
                  warehouse_table.update(t_i, payload);
                  goto end;
                }
                rnd -= 430;
                if (rnd < 40) {
                  order_status_table.update(t_i * distance, payload);
                  goto end;
                }
                rnd -= 40;
                if (rnd < 40) {
                  delivery_table.update(t_i * distance, payload);
                  goto end;
                }
                rnd -= 40;
                if (rnd < 40) {
                  stock_table.update(t_i * distance, payload);
                  goto end;
                }
                rnd -= 40;
                new_order_table.update(t_i * distance, payload);
              }
            end:
              WorkerCounters::myCounters().tx++;
            }
            running_threads_counter--;
          },
          t_i);
    }
  // -------------------------------------------------------------------------------------
  {
    // Shutdown threads
    sleep(FLAGS_run_for_seconds);
    keep_running = false;
    // -------------------------------------------------------------------------------------
    while (running_threads_counter) {
      _mm_pause();
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }
  // -------------------------------------------------------------------------------------
  return 0;
}
