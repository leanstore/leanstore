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
atomic<u64> Worker::global_logical_clock = WORKERS_INCREMENT;
atomic<u64> Worker::global_gsn_flushed = 0;
atomic<u64> Worker::global_sync_to_this_gsn = 0;
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
    : cc(history_tree), worker_id(worker_id), all_workers(all_workers), workers_count(workers_count), ssd_fd(fd), is_page_provider(is_page_provider)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   std::memset(logging.wal_buffer, 0, WORKER_WAL_SIZE);
   if (!is_page_provider) {
      cc.local_snapshot_cache = make_unique<u64[]>(workers_count);
      cc.local_snapshot_cache_ts = make_unique<u64[]>(workers_count);
      cc.local_workers_start_ts = make_unique<u64[]>(workers_count + 1);
      global_workers_current_snapshot[worker_id] = 0;
   }
}
Worker::~Worker() = default;
// -------------------------------------------------------------------------------------
void Worker::startTX(TX_MODE next_tx_type, TX_ISOLATION_LEVEL next_tx_isolation_level, bool read_only)
{
   Transaction prev_tx = active_tx;
   // For single-statement transactions, snapshot isolation and serialization are the same as read committed
   if (next_tx_type == TX_MODE::SINGLE_STATEMENT && 0) {  // TODO: check consequences on refreshGlobalState & GC
      if (next_tx_isolation_level > TX_ISOLATION_LEVEL::READ_COMMITTED) {
         next_tx_isolation_level = TX_ISOLATION_LEVEL::READ_COMMITTED;
      }
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal) {
      logging.current_tx_wal_start = logging.wal_wt_cursor;
      if (!read_only) {
         WALMetaEntry& entry = logging.reserveWALMetaEntry();
         entry.type = WALEntry::TYPE::TX_START;
         logging.submitWALMetaEntry();
      }
      assert(prev_tx.state != Transaction::STATE::STARTED);
      // -------------------------------------------------------------------------------------
      // Initialize RFA
      if (FLAGS_wal_rfa) {
         const LID sync_point = Worker::global_sync_to_this_gsn.load();
         if (sync_point > logging.getCurrentGSN()) {
            logging.setCurrentGSN(sync_point);
            logging.publishMaxGSNOffset();
         }
         logging.rfa_gsn_flushed = Worker::global_gsn_flushed.load();
         logging.needs_remote_flush = false;
      } else {
         logging.needs_remote_flush = true;
      }
      // -------------------------------------------------------------------------------------
      active_tx.state = Transaction::STATE::STARTED;
      active_tx.has_wrote = false;
      active_tx.min_observed_gsn_when_started = logging.clock_gsn;
      active_tx.current_tx_mode = next_tx_type;
      active_tx.current_tx_isolation_level = next_tx_isolation_level;
      active_tx.is_read_only = read_only;
      active_tx.is_durable = FLAGS_wal;  // TODO:
      // -------------------------------------------------------------------------------------
      // Draw TXID from global counter and publish it with the TX type (i.e., OLAP or OLTP)
      // We have to acquire a transaction id and use it for locking in ANY isolation level
      if (next_tx_isolation_level >= TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION) {
         // Also means multi statement
         global_workers_current_snapshot[worker_id].store(active_tx.tx_id | LATCH_BIT, std::memory_order_release);
         active_tx.tx_id = global_logical_clock.fetch_add(1);
         if (FLAGS_olap_mode) {
            global_workers_current_snapshot[worker_id].store(active_tx.tx_id | ((active_tx.isOLAP()) ? OLAP_BIT : 0), std::memory_order_release);
         } else {
            global_workers_current_snapshot[worker_id].store(active_tx.tx_id, std::memory_order_release);
         }
         cc.local_global_all_lwm_cache = global_all_lwm.load();
         // -------------------------------------------------------------------------------------
         if (prev_tx.isReadCommitted() || prev_tx.isReadUncommitted()) {
            cc.switchToSnapshotIsolationMode();
         }
         // -------------------------------------------------------------------------------------
      } else {
         if (prev_tx.atLeastSI()) {
            cc.switchToReadCommittedMode();
         }
         if (next_tx_type != TX_MODE::SINGLE_STATEMENT) {
            active_tx.tx_id = global_logical_clock.fetch_add(1);
         }
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   if (activeTX().isDurable()) {
      command_id = 0;  // Reset command_id only on commit and never on abort
      // -------------------------------------------------------------------------------------
      if (activeTX().isSingleStatement() && active_tx.state != Transaction::STATE::STARTED) {
         return;  // Skip double commit in case of single statement upsert [hack]
      }
      assert(active_tx.state == Transaction::STATE::STARTED);
      // -------------------------------------------------------------------------------------
      if (!activeTX().isReadOnly()) {
         WALMetaEntry& entry = logging.reserveWALMetaEntry();
         entry.type = WALEntry::TYPE::TX_COMMIT;
         logging.submitWALMetaEntry();
      }
      // -------------------------------------------------------------------------------------
      active_tx.max_observed_gsn = logging.clock_gsn;
      active_tx.state = Transaction::STATE::READY_TO_COMMIT;
      // We don't have pmem, so we can only avoid remote flushes for real if the tx is a single lookup
      if (!FLAGS_wal_rfa_pmem_simulate) {
         logging.needs_remote_flush |= !activeTX().isReadOnly();
      }
      if (logging.needs_remote_flush) {  // RFA
         std::unique_lock<std::mutex> g(logging.precommitted_queue_mutex);
         logging.precommitted_queue.push_back(active_tx);
         logging.ready_to_commit_queue_size += 1;
      } else {
         active_tx.state = Transaction::STATE::COMMITTED;
         CRCounters::myCounters().rfa_committed_tx += 1;
      }
      // -------------------------------------------------------------------------------------
      if (activeTX().hasWrote()) {
         TXID commit_ts;
         cc.commit_tree->append(
             [&](u8* key) {
                commit_ts = global_logical_clock.fetch_add(1);
                utils::fold(key, commit_ts);
             },
             sizeof(TXID), [&](u8* value) { *reinterpret_cast<TXID*>(value) = active_tx.TTS(); }, sizeof(TXID), cc.commit_tree_handler);
         cc.local_latest_write_tx.store(commit_ts, std::memory_order_release);
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
   if (FLAGS_wal) {
      ensure(active_tx.state == Transaction::STATE::STARTED);
      const u64 tx_id = active_tx.TTS();
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
      cc.history_tree.purgeVersions(worker_id, active_tx.TTS(), active_tx.TTS(), [&](const TXID, const DTID, const u8*, u64, const bool) {});
      // -------------------------------------------------------------------------------------
      WALMetaEntry& entry = logging.reserveWALMetaEntry();
      entry.type = WALEntry::TYPE::TX_ABORT;
      logging.submitWALMetaEntry();
      active_tx.state = Transaction::STATE::ABORTED;
   }
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
