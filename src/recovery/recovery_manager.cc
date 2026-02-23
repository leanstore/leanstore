#include "recovery/recovery_manager.h"
#include "buffer/buffer_manager.h"
#include "leanstore/leanstore.h"
#include "storage/btree/node.h"
#include "storage/btree/wal.h"
#include "sync/page_guard/exclusive_guard.h"

#include "fmt/format.h"

#include <cstring>

using leanstore::storage::WALAfterImage;
using leanstore::storage::WALDeltaImage;
using leanstore::storage::WALEntry;
using leanstore::storage::WALFreshPage;
using leanstore::storage::WALInsert;
using leanstore::storage::WALInsertSep;
using leanstore::storage::WALLogicalSplit;
using leanstore::storage::WALNewPage;
using leanstore::storage::WALNewRoot;
using leanstore::storage::WALRemove;
using leanstore::storage::WalType;
using leanstore::sync::ExclusiveGuard;

namespace leanstore::recovery {

// -------------------------------------------------------------------------------------

/**
 * @brief TODO(XXX): For now, assume that the recovery session and the previous LeanStore session
 *    have the same number of workers
 */
RecoveryManager::RecoveryManager(buffer::BufferManager *buffer_pool)
    : buffer_(buffer_pool), backend_(buffer_), max_logged_pid_(0) {}

void RecoveryManager::RecoveryPreparation() {
  backend_.Connect();
  backend_.RetrieveMasterRecord();
}

auto RecoveryManager::SignatureOfPrevSession() -> u32 { return backend_.signature_; }

/**
 * @brief It's possible that there are a few empty log blocks before the last log blocks
 * This is because some workers fail to write something to their assigned block before the crash
 *
 * The upper-bound of the possible number of empty blocks is the number of workers (in previous session)
 * As a result, we will retry at least # workers empty log blocks to detect tail of the log
 *
 * After this phase, page_log_[] become read-only
 */
auto RecoveryManager::MaterializeLogs() -> u64 {
  auto last_valid_offset = backend_.ReadLogBlocks([&](u8 *buffer, u64 buffer_len, u32 signature) {
    /**
     * @brief The `buffer` now stores a sequence of `LogIOSegment`,
     *  which we will iterate through and materialize them.
     * Each iteration requires verifying the LogIOSegment signature
     */
    for (auto ptr = buffer_len; ptr > 0;) {
      auto segment_sz = LogIOSegment::Deserialize(buffer, ptr, signature, [&](u8 *segment_buf, u64 buf_len) {
        AnalyzeLogSegment(LogIOSegment::Buffer(segment_buf, buf_len));
      });
      if (segment_sz == 0) { break; }
      ptr -= segment_sz;
    }
  });

  return last_valid_offset;
}

void RecoveryManager::Analysis() {
  std::unordered_set<txnid_t> losers;

  /* Detect real losers from possible thread-local losers */
  for (auto &txn : tl_losers_[LeanStore::worker_thread_id]) {
    if (!winners_.contains(txn)) { losers.insert(txn); }
  }
  tl_losers_[LeanStore::worker_thread_id].clear();

  /* Detect losers from possible winners, i.e., if they access dirty data from other workers */
  for (auto &[txn, dep_gsn] : tl_winners_[LeanStore::worker_thread_id]) {
    /* Check whether it depends on a un-logged data */
    for (auto &[wid, ts] : dep_gsn) {
      if (ts > global_durable_gsn_[wid].load()) {
        losers.insert(txn);
        break;
      }
    }
  }

  // Bulk insertion to global list
  losers_.insert(std::begin(losers), std::end(losers));
}

/* Should only be triggered by one thread */
void RecoveryManager::PrepareRedoEnv() {
  has_recovered_ = std::make_unique<std::atomic<bool>[]>(max_logged_pid_.load() + 1);
  for (auto pid = 1U; pid <= max_logged_pid_; pid++) {
    has_recovered_[pid] = false;
    auto page           = buffer_->AllocPage();
    Ensure(buffer_->ToPID(page) == pid);
    buffer_->GetPageState(pid).UnlockExclusive();
  }
  {
    /* Always redo the metadata page first */
    ExclusiveGuard<storage::MetadataPage> meta_page(buffer_, METADATA_PAGE_ID);
    PerPageRedo(meta_page, METADATA_PAGE_ID);
  }
}

auto RecoveryManager::HasRecovered(pageid_t pid) -> bool {
  return (FLAGS_wal_enable_recovery) && ((pid > max_logged_pid_) || (has_recovered_[pid].load()));
}

/**
 * @brief TODO(XXX): The current recovery impl does not support MVCC implementation.
 * That is, to fully support MVCC, we need to properly recover tuple's timestamp.
 * Reason: Tuple's timestamp is only recorded in the COMMIT_TX log entry, not in data log entries.
 */
template <class BTreeNode>
void RecoveryManager::PerPageRedo(ExclusiveGuard<BTreeNode> &page, pageid_t pid) {
  /* MapReduce */
  LogIOSegment::LazyGenerator log_generator;
  for (auto wid = 0U; wid < FLAGS_wal_recovery_threads; wid++) {
    if (page_log_[wid].contains(pid)) {
      for (auto &buffer : page_log_[wid][pid]) { log_generator.ReceiveInput(&buffer); }
    }
  }
  log_generator.Iterate([&](const DataEntry *log) {
    assert(log->type == LogEntry::Type::DATA_ENTRY);
    if (losers_.contains(log->rc.txn)) {
      /**
       * @brief The current log action belongs to a loser, hence all the succeeding actions
       *  should also belongs to loser transactions as well
       */
      return false;
    }
    RedoLog(page, log);
    return true;
  });
  has_recovered_[pid] = true;
  /* Free those log buffers */
  for (auto wid = 0U; wid < FLAGS_wal_recovery_threads; wid++) {
    if (page_log_[wid].contains(pid)) {
      for (auto &buffer : page_log_[wid][pid]) { buffer.Deallocate(); }
    }
  }
}

void RecoveryManager::Redo() {
  if (FLAGS_wal_instant_recovery || max_logged_pid_ == 0) { return; }
  for (auto pid = 1U; pid <= max_logged_pid_; pid++) {
    if (pid % FLAGS_wal_recovery_threads == LeanStore::worker_thread_id) {
      ExclusiveGuard<storage::BTreeNode> page(buffer_, pid);
      PerPageRedo(page, pid);
    }
  }
}

void RecoveryManager::Undo() {
  if (FLAGS_wal_instant_recovery) { return; }
  /**
   * @brief Unnecessary because LeanStore doesn't support isolations higher than Read uncommitted
   *  => No transaction is aborted
   */
  throw leanstore::ex::TODO("UNDO is not supported at the moment");
}

// -------------------------------------------------------------------------------------

/* Merge sorts and analyze all log segments */
void RecoveryManager::AnalyzeLogSegment(const LogIOSegment::Buffer &tmp) {
  std::unordered_map<pageid_t, std::unordered_map<wid_t, LogIOSegment::Buffer>> tmp_analysis;
  tmp.Iterate([&](const LogEntry *log) {
    switch (log->type) {
      case LogEntry::Type::TX_START:
        tl_losers_[LeanStore::worker_thread_id].emplace(reinterpret_cast<const LogMetaEntry *>(log)->rc.txn);
        break;
      case LogEntry::Type::TX_COMMIT: {
        auto mt = reinterpret_cast<const TxnCommitEntry *>(log);
        winners_.insert(mt->rc.txn);
        tl_winners_[LeanStore::worker_thread_id].insert({mt->rc.txn, {}});
        /* Materialize the GSN Vector */
        auto wid = reinterpret_cast<const wid_t *>(mt->payload);
        auto ts  = reinterpret_cast<const timestamp_t *>(&(mt->payload)[sizeof(wid_t) * mt->vector_size]);
        UpdateMax(global_durable_gsn_[mt->rc.w_id], mt->rc.gsn); /* Evaluate the maximum global durable GSN */
        auto &durable_gsn = tl_winners_[LeanStore::worker_thread_id][mt->rc.txn];
        for (auto idx = 0U; idx < mt->vector_size; idx++) { durable_gsn[wid[idx]] = ts[idx]; }
      } break;
      case LogEntry::Type::DATA_ENTRY: {
        auto wal = reinterpret_cast<const DataEntry *>(log);
        tmp_analysis[wal->pid][wal->rc.w_id].InsertNewLog(reinterpret_cast<const u8 *>(wal), wal->size);
      } break;
      case LogEntry::Type::TX_ABORT: throw leanstore::ex::TODO("Recovery for ABORT is not yet implemented"); break;
      case LogEntry::Type::FREE_EXTENT:
      case LogEntry::Type::REUSE_EXTENT:
      case LogEntry::Type::PAGE_IMG:
        /* TODO: Not yet support recover these actions */
        throw leanstore::ex::TODO("BLOB recovery is not yet implemented");
      case LogEntry::Type::MASTER_RECORD:
      case LogEntry::Type::WRITE_METADATA:
        /* Logging metadata should be handled/eliminated during log materialization */
        throw leanstore::ex::Unreachable("Shouldn't be here");
      case LogEntry::Type::CARRIAGE_RETURN:
        /* Doesn't have to do anything in this case */
        break;
      default: throw leanstore::ex::Unreachable("All cases should be handled above"); break;
    }
    return true;
  });
  for (auto &[pid, w_segment] : tmp_analysis) {
    UpdateMax(max_logged_pid_, pid);
    auto &log = page_log_[LeanStore::worker_thread_id];
    for (auto &[wid, segment] : w_segment) { log[pid].emplace_back(std::move(segment)); }
  }
}

template <>
void RecoveryManager::RedoLog(ExclusiveGuard<storage::MetadataPage> &page, const recovery::DataEntry *log) {
  assert(page->p_gsn < log->rc.gsn);
  auto entry = reinterpret_cast<const WALEntry *>(log);
  switch (entry->wal_type) {
    case WalType::NEW_ROOT: {
      auto init = reinterpret_cast<const WALNewRoot *>(entry);
      Ensure(init->pid == 0);
      auto meta_p                  = reinterpret_cast<storage::MetadataPage *>(page.Ptr());
      meta_p->roots[init->slot_id] = init->new_root_pid;
    } break;
    default: throw leanstore::ex::Unreachable("All cases should be handled above"); break;
  }
  // Increase page GSN
  page->p_gsn = log->rc.gsn;
}

template <>
void RecoveryManager::RedoLog(ExclusiveGuard<storage::BTreeNode> &page, const recovery::DataEntry *log) {
  assert(page->p_gsn < log->rc.gsn);
  auto entry = reinterpret_cast<const WALEntry *>(log);
  switch (entry->wal_type) {
    case WalType::INSERT: {
      auto ins = reinterpret_cast<const WALInsert *>(entry);
      auto key = std::span<u8>(const_cast<u8 *>(ins->payload), ins->key_length);
      auto payload =
        std::span<const u8>(reinterpret_cast<const u8 *>(ins->payload + ins->key_length), ins->value_length);
      Ensure(page->HasSpaceForKV(key.size(), payload.size()));
      page->InsertKeyValue(key, payload, cmp_lambda_);
    } break;
    case WalType::INSERT_SEP: {
      auto sep = reinterpret_cast<const WALInsertSep *>(entry);
      page->InsertSeparator({const_cast<u8 *>(sep->sep_key), sep->sep_length}, sep->left_pid, sep->right_pid,
                            cmp_lambda_);
    } break;
    case WalType::LOGICAL_SPLIT: {
      auto split   = reinterpret_cast<const WALLogicalSplit *>(entry);
      auto sep_key = std::vector<u8>(split->sep.len);
      page->GetSeparatorKey(sep_key.data(), split->sep);
      page->SplitNode(nullptr, nullptr, page.PageID(), split->next_leaf_node, split->sep.slot,
                      {sep_key.data(), split->sep.len}, cmp_lambda_);
    } break;
    case WalType::FRESH_PAGE: {
      auto fresh = reinterpret_cast<const WALFreshPage *>(entry);
      new (page.Ptr()) storage::BTreeNode(!fresh->is_inner);
      if (fresh->is_inner) { page->header.right_most_child = fresh->right_most_child; }
    } break;
    case WalType::NEW_PAGE: {
      auto init = reinterpret_cast<const WALNewPage *>(entry);
      auto ptr  = reinterpret_cast<u8 *>(buffer_->ToPtr(init->pid));
      std::memcpy(ptr, init->payload, init->metadata_size);
      std::memcpy(ptr + PAGE_SIZE - init->payload_size, init->payload + init->metadata_size, init->payload_size);
    } break;
    case WalType::REMOVE: {
      auto rm = reinterpret_cast<const WALRemove *>(entry);
      page->RemoveSlot(rm->slot_id);
    } break;
    case WalType::AFTER_IMAGE: {
      auto ai = reinterpret_cast<const WALAfterImage *>(entry);
      bool found;
      auto pos = page->LowerBound({const_cast<u8 *>(ai->payload), ai->key_length}, found, cmp_lambda_);
      Ensure(found);
      auto record = page->GetPayload(pos);
      Ensure(record.size() == ai->value_length);
      std::memcpy(record.data(), ai->payload + ai->key_length, record.size());
    } break;
    case WalType::DELTA_IMAGE: {
      auto dt_log = reinterpret_cast<const WALDeltaImage *>(entry);
      auto delta  = reinterpret_cast<const FixedSizeDelta *>(dt_log->payload + dt_log->key_length);
      bool found;
      auto pos = page->LowerBound({const_cast<u8 *>(dt_log->payload), dt_log->key_length}, found, cmp_lambda_);
      Ensure(found);
      auto record = page->GetPayload(pos);
      delta->ApplyDelta(record);
    } break;
    default: throw leanstore::ex::Unreachable("All cases should be handled above"); break;
  }
  // Increase page GSN
  page->p_gsn = log->rc.gsn;
}

template void RecoveryManager::PerPageRedo<storage::BTreeNode>(ExclusiveGuard<storage::BTreeNode> &page, pageid_t pid);
template void RecoveryManager::PerPageRedo<storage::MetadataPage>(ExclusiveGuard<storage::MetadataPage> &page,
                                                                  pageid_t pid);

}  // namespace leanstore::recovery