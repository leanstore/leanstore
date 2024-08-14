#include "Worker.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <stdio.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <sstream>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
thread_local Worker* Worker::tls_ptr = nullptr;
std::shared_mutex Worker::global_mutex;  // Unused
// -------------------------------------------------------------------------------------
std::unique_ptr<atomic<u64>[]> Worker::global_workers_current_snapshot;  // All transactions < are committed
atomic<u64> Worker::global_oldest_all_start_ts = 0;
atomic<u64> Worker::global_oldest_oltp_start_ts = 0;
atomic<u64> Worker::global_all_lwm = 0;
atomic<u64> Worker::global_oltp_lwm = 0;
atomic<u64> Worker::global_newest_olap_start_ts = 0;
// -------------------------------------------------------------------------------------
Worker::Worker(u64 worker_id, Worker** all_workers, u64 workers_count, HistoryTreeInterface& history_tree, s32 fd, const bool is_page_provider)
    : cc(history_tree, workers_count),
      worker_id(worker_id),
      all_workers(all_workers),
      workers_count(workers_count),
      ssd_fd(fd),
      is_page_provider(is_page_provider)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   logging.wal_buffer = reinterpret_cast<u8*>(std::aligned_alloc(512, FLAGS_wal_buffer_size));
   std::memset(logging.wal_buffer, 0, FLAGS_wal_buffer_size);
   if (!is_page_provider) {
      cc.local_snapshot_cache = make_unique<u64[]>(workers_count);
      cc.local_snapshot_cache_ts = make_unique<u64[]>(workers_count);
      cc.local_workers_start_ts = make_unique<u64[]>(workers_count + 1);
      global_workers_current_snapshot[worker_id] = 0;
   }
   cc.wt_pg.local_workers_tx_id = std::make_unique<std::atomic<TXID>[]>(workers_count);
}
Worker::~Worker()
{
   delete[] cc.commit_tree.array;
   std::free(logging.wal_buffer);
   logging.wal_buffer = nullptr;
}
// -------------------------------------------------------------------------------------
void Worker::startTX(TX_MODE next_tx_type, TX_ISOLATION_LEVEL next_tx_isolation_level, bool read_only)
{
   utils::Timer timer(CRCounters::myCounters().cc_ms_start_tx);
   Transaction prev_tx = active_tx;
   active_tx.stats.start = std::chrono::high_resolution_clock::now();
   if (FLAGS_wal) {
      active_tx.wal_larger_than_buffer = false;
      logging.current_tx_wal_start = logging.wal_wt_cursor;
      if (!read_only) {
         WALMetaEntry& entry = logging.reserveWALMetaEntry();
         entry.type = WALEntry::TYPE::TX_START;
         logging.submitWALMetaEntry();
         DEBUG_BLOCK() { entry.checkCRC(); }
      }
      assert(prev_tx.state != Transaction::STATE::STARTED);
      // -------------------------------------------------------------------------------------
      const LID sync_point = Worker::Logging::global_sync_to_this_gsn.load();
      if (sync_point > logging.getCurrentGSN()) {
         logging.setCurrentGSN(sync_point);
         logging.publishMaxGSNOffset();
      }
      if (FLAGS_wal_rfa) {
         logging.rfa_gsn_flushed = Worker::Logging::global_min_gsn_flushed.load();
         logging.remote_flush_dependency = false;
      } else {
         logging.remote_flush_dependency = true;
      }
      // -------------------------------------------------------------------------------------
      active_tx.state = Transaction::STATE::STARTED;
      active_tx.has_wrote = false;
      active_tx.min_observed_gsn_when_started = logging.wt_gsn_clock;
      active_tx.current_tx_mode = next_tx_type;
      active_tx.current_tx_isolation_level = next_tx_isolation_level;
      active_tx.is_read_only = read_only;
      active_tx.is_durable = FLAGS_wal;  // TODO:
      // -------------------------------------------------------------------------------------
      // Draw TXID from global counter and publish it with the TX type (i.e., OLAP or OLTP)
      // We have to acquire a transaction id and use it for locking in ANY isolation level
      if (next_tx_isolation_level >= TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION) {  // implies multi-statement
         if (prev_tx.isReadCommitted() || prev_tx.isReadUncommitted()) {
            cc.switchToSnapshotIsolationMode();
         }
         // -------------------------------------------------------------------------------------
         {
           utils::Timer timer(CRCounters::myCounters().cc_ms_snapshotting);
           global_workers_current_snapshot[worker_id].store(active_tx.start_ts | LATCH_BIT, std::memory_order_release);
           active_tx.start_ts = ConcurrencyControl::global_clock.fetch_add(1);
           if (FLAGS_olap_mode) {
             global_workers_current_snapshot[worker_id].store(active_tx.start_ts | ((active_tx.isOLAP()) ? OLAP_BIT : 0), std::memory_order_release);
           } else {
             global_workers_current_snapshot[worker_id].store(active_tx.start_ts, std::memory_order_release);
           }
         }
         cc.commit_tree.cleanIfNecessary();
         cc.local_global_all_lwm_cache = global_all_lwm.load();
      } else {
        if (prev_tx.atLeastSI()) {
          cc.switchToReadCommittedMode();
        }
        cc.commit_tree.cleanIfNecessary();
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
  if (activeTX().isDurable()) {
    {
      utils::Timer timer(CRCounters::myCounters().cc_ms_commit_tx);
      command_id = 0;  // Reset command_id only on commit and never on abort
      // -------------------------------------------------------------------------------------
      assert(active_tx.state == Transaction::STATE::STARTED);
      // -------------------------------------------------------------------------------------
      if (FLAGS_wal_tuple_rfa) {
        for (auto& dependency : logging.rfa_checks_at_precommit) {
          if (logging.other(std::get<0>(dependency)).signaled_commit_ts < std::get<1>(dependency)) {
            logging.remote_flush_dependency = true;
            break;
          }
        }
        logging.rfa_checks_at_precommit.clear();
      }
      // -------------------------------------------------------------------------------------
      if (activeTX().hasWrote()) {
        TXID commit_ts = cc.commit_tree.commit(active_tx.startTS());
        cc.local_latest_write_tx.store(commit_ts, std::memory_order_release);
        active_tx.commit_ts = commit_ts;
      }
      // -------------------------------------------------------------------------------------
      active_tx.max_observed_gsn = logging.wt_gsn_clock;
      active_tx.state = Transaction::STATE::READY_TO_COMMIT;
      // -------------------------------------------------------------------------------------
      WALMetaEntry& entry = logging.reserveWALMetaEntry();
      entry.type = WALEntry::TYPE::TX_COMMIT;
      // TODO: commit_ts in log
      logging.submitWALMetaEntry();
      if (FLAGS_wal_variant == 2) {
        logging.wt_to_lw.optimistic_latch.notify_all();
      }
      // -------------------------------------------------------------------------------------
      active_tx.stats.precommit = std::chrono::high_resolution_clock::now();
      std::unique_lock<std::mutex> g(logging.precommitted_queue_mutex);
      if (logging.remote_flush_dependency) {  // RFA
        logging.precommitted_queue.push_back(active_tx);
      } else {
        CRCounters::myCounters().rfa_committed_tx++;
        logging.precommitted_queue_rfa.push_back(active_tx);
      }
    }
    // Only committing snapshot/ changing between SI and lower modes
    if (activeTX().atLeastSI()) {
      cc.refreshGlobalState();
    }
    // All isolation level generate garbage
    cc.garbageCollection();
  }
}
// -------------------------------------------------------------------------------------
void Worker::abortTX()
{
   utils::Timer timer(CRCounters::myCounters().cc_ms_abort_tx);
   ensure(FLAGS_wal);
   ensure(!active_tx.wal_larger_than_buffer);
   ensure(active_tx.state == Transaction::STATE::STARTED);
   const u64 tx_id = active_tx.startTS();
   std::vector<const WALEntry*> entries;
   logging.iterateOverCurrentTXEntries([&](const WALEntry& entry) {
      if (entry.type == WALEntry::TYPE::DT_SPECIFIC) {
         entries.push_back(&entry);
      }
   });
   std::for_each(entries.rbegin(), entries.rend(), [&](const WALEntry* entry) {
      const auto& dt_entry = *reinterpret_cast<const WALDTEntry*>(entry);
      leanstore::storage::DTRegistry::global_dt_registry.undo(dt_entry.dt_id, dt_entry.payload, tx_id);
   });
   // -------------------------------------------------------------------------------------
   cc.history_tree.purgeVersions(worker_id, active_tx.startTS(), active_tx.startTS(), [&](const TXID, const DTID, const u8*, u64, const bool) {});
   // -------------------------------------------------------------------------------------
   WALMetaEntry& entry = logging.reserveWALMetaEntry();
   entry.type = WALEntry::TYPE::TX_ABORT;
   logging.submitWALMetaEntry();
   active_tx.state = Transaction::STATE::ABORTED;
   jumpmu::jump();
}
// -------------------------------------------------------------------------------------
void Worker::shutdown()
{
   cc.garbageCollection();
   cc.switchToReadCommittedMode();
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
