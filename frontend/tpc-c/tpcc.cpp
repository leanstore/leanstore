#include "Time.hpp"
#include "adapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/ThreadCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "schema.hpp"
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency/Mean.hpp"
#include "leanstore/io/IoInterface.hpp"

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <iostream>
#include <set>
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_uint32(tpcc_warehouse_count, 1, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
LeanStoreAdapter<warehouse_t> warehouse;
LeanStoreAdapter<district_t> district;
LeanStoreAdapter<customer_t> customer;
LeanStoreAdapter<customer_wdl_t> customerwdl;
LeanStoreAdapter<history_t> history;
LeanStoreAdapter<neworder_t> neworder;
LeanStoreAdapter<order_t> order;
LeanStoreAdapter<order_wdc_t> order_wdc;
LeanStoreAdapter<orderline_t> orderline;
LeanStoreAdapter<item_t> item;
LeanStoreAdapter<stock_t> stock;
// -------------------------------------------------------------------------------------
// yeah, dirty include...
#include "tpcc_workload.hpp"
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}

void run_tpcc()
{
   LeanStore db;
   //auto& crm = db.getCRManager();
   // -------------------------------------------------------------------------------------
   warehouseCount = FLAGS_tpcc_warehouse_count;
   mean::task::scheduleTaskSync([&]() {
     warehouse = LeanStoreAdapter<warehouse_t>(db, "warehouse", "w");
      district = LeanStoreAdapter<district_t>(db, "district", "d");
      customer = LeanStoreAdapter<customer_t>(db, "customer", "c");
      customerwdl = LeanStoreAdapter<customer_wdl_t>(db, "customerwdl", "k");
      history = LeanStoreAdapter<history_t>(db, "history", "h");
      neworder = LeanStoreAdapter<neworder_t>(db, "neworder", "n");
      order = LeanStoreAdapter<order_t>(db, "order", "o");
      order_wdc = LeanStoreAdapter<order_wdc_t>(db, "order_wdc", "r");
      orderline = LeanStoreAdapter<orderline_t>(db, "orderline", "l");
      item = LeanStoreAdapter<item_t>(db, "item", "i");
      stock = LeanStoreAdapter<stock_t>(db, "stock", "s");
   });
   // -------------------------------------------------------------------------------------
   db.registerConfigEntry("tpcc_warehouse_count", FLAGS_tpcc_warehouse_count);
   db.registerConfigEntry("tpcc_warehouse_affinity", FLAGS_tpcc_warehouse_affinity);
   db.registerConfigEntry("run_until_tx", FLAGS_run_until_tx);
   for (auto& t: DTRegistry::global_dt_registry.dt_instances_ht) {
      std::cout << "dt_ids: " << t.first << " name: " << std::get<2>(t.second) << " short: " << std::get<3>(t.second) << std::endl;
   }
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   // const u64 load_threads = (FLAGS_tpcc_fast_load) ? thread::hardware_concurrency() : FLAGS_worker_threads;
   {
      mean::task::scheduleTaskSync([&]() {
         //cr::Worker::my().startTX();
         loadItem();
         loadWarehouse();
         //cr::Worker::my().commitTX();
      });

      auto load_fun = [](mean::BlockedRange bb, std::atomic<bool>&) {
         ensure(bb.begin < 1000000);
         for (u64 w_id = bb.begin; w_id < bb.end; w_id += 1) {
            //cr::Worker::my().startTX();
            loadStock(w_id);
            loadDistrinct(w_id);
            for (Integer d_id = 1; d_id <= 10; d_id++) {
               loadCustomer(w_id, d_id);
               loadOrders(w_id, d_id);
            }
            //cr::Worker::my().commitTX();
         }
      };
      mean::BlockedRange bb(1, (u64)FLAGS_tpcc_warehouse_count + 1);
      ensure((bool)((bb.end - bb.begin) > 0));
         auto before = mean::getTimePoint();
      mean::task::parallelFor(bb, load_fun, 1);
      auto timeDiff = mean::timePointDifference(mean::getTimePoint(), before);
      std::cout << "Loading done in: " + std::to_string(timeDiff/1e9f) + "s" << std::endl;
   }
   sleep(2);
   // -------------------------------------------------------------------------------------
   double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << "data loaded - consumed space in GiB = " << gib << endl;
   mean::task::scheduleTaskSync([&]() { cout << "Warehouse pages = " << warehouse.btree->countPages() << endl; });
   // -------------------------------------------------------------------------------------
   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_tpcc_warehouse_count, FLAGS_zipf_factor);
   mean::env::adjustWorkerCount(FLAGS_worker_threads);
   u64 tx_per_thread[FLAGS_worker_threads];
   auto start = mean::getSeconds();
   auto tpcc_fun = [&running_threads_counter, &keep_running, &start](mean::BlockedRange bb, std::atomic<bool>& cancelled) {
       int thr = running_threads_counter++;
       volatile u64 tx_acc = 0;
       //cr::Worker::my().refreshSnapshot();
       volatile u64 i = bb.begin;
       char* buf = static_cast<char*>(mean::IoInterface::allocIoMemoryChecked(4*1024, 512));

       int wc = FLAGS_tpcc_warehouse_count / (mean::env::workerCount() * FLAGS_worker_tasks);
       int id = mean::exec::getId()*wc + 1;
       u64 rateLimitngEveryNs =  1;///3*1000*1000;
       u64 lastTsc = mean::readTSC();

       //std::cout << "w_i: " << id << " to: " << id+wc << std::endl;
       while (i < bb.end && keep_running) {
          if (mean::getSeconds() - start > FLAGS_run_for_seconds) {
            cancelled = true;
            break;
          }
           auto before = mean::getTimePoint();
           jumpmuTry()
           {
              //cr::Worker::my().startTX();
              u32 w_id;
              if (FLAGS_tpcc_warehouse_affinity) {
                 //w_id = urand(id,id+wc);
                 w_id = (thr -1) % FLAGS_tpcc_warehouse_count + 1;
              } else {
                 w_id = urand(1, FLAGS_tpcc_warehouse_count);
              }
              //std::cout << "run tx" << std::endl;
              tx(w_id);

              //long len = 4*1024;
              //long max = 10ul*1024*1024*1024/len;
              //long addr =  urand(1, max) * len;
              //mean::task::read(buf, urand(1, max), len);
              //std::cout << "r: " << addr << " l: " << len << std::endl;

              //std::cout << "end tx" << std::endl;
              if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                 //cr::Worker::my().abortTX();
              } else {
                 //cr::Worker::my().commitTX();
              }
              tx_acc++;
           }
           jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; ThreadCounters::myCounters().tx_abort++; }
           i++;
           auto timeDiff = mean::timePointDifferenceUs(mean::getTimePoint(), before);
           WorkerCounters::myCounters().total_tx_time += timeDiff;
           WorkerCounters::myCounters().tx_latency_hist.increaseSlot(timeDiff);
           WorkerCounters::myCounters().tx++;
           ThreadCounters::myCounters().tx++;
           mean::task::yield();
           /*
           while (true) { 
              mean::task::yield();
              u64 now = mean::readTSC();
              if ((now - lastTsc) / 2 > rateLimitngEveryNs) {
                 lastTsc = now;
                 break;
              }
           }
           */
       }
     //tx_per_thread[t_i] = tx_acc; // fixme
     running_threads_counter--;
   };
   mean::BlockedRange bb(0, (u64)FLAGS_run_until_tx);
   ensure((bool)((bb.end - bb.begin) > 1));
   start = mean::getSeconds();
   mean::task::parallelFor(bb, tpcc_fun, FLAGS_worker_tasks, 100000);
   if (FLAGS_persist) {
      db.getBufferManager().writeAllBufferFrames();
   }
   // -------------------------------------------------------------------------------------
   gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << endl << "consumed space in GiB = " << gib << endl;
   cout << "consumed space in GiB = " << gib << endl;
   // -------------------------------------------------------------------------------------
   if (FLAGS_persist) {
      db.getBufferManager().writeAllBufferFrames();
   }
   mean::env::shutdown();
}

// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   using namespace mean;
   IoOptions ioOptions("auto", FLAGS_ssd_path);
   ioOptions.write_back_buffer_size = PAGE_SIZE;
   ioOptions.engine = FLAGS_ioengine;
   ioOptions.ioUringPollMode = FLAGS_io_uring_poll_mode;
   ioOptions.ioUringShareWq = FLAGS_io_uring_share_wq;
   ioOptions.raid5 = FLAGS_raid5;
   ioOptions.iodepth = (FLAGS_async_batch_size + FLAGS_worker_tasks); // hacky, how to take into account for remotes 
   // -------------------------------------------------------------------------------------
   if (FLAGS_nopp) {
      ioOptions.channelCount = FLAGS_worker_threads;
      mean::env::init(
         FLAGS_worker_threads, //std::min(std::thread::hardware_concurrency(), FLAGS_tpcc_warehouse_count),
         0/*FLAGS_pp_threads*/, ioOptions);
   } else {
      ioOptions.channelCount = FLAGS_worker_threads + FLAGS_pp_threads;
      mean::env::init(FLAGS_worker_threads, FLAGS_pp_threads, ioOptions);
   }
   mean::env::start(run_tpcc);
   // -------------------------------------------------------------------------------------
   mean::env::join();
   // -------------------------------------------------------------------------------------
   return 0;
}
