#include "BTreeLL.hpp"

#include "core/BTreeGenericIterator.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->sanityCheck(key, key_length);
            leaf.recheck_done();
            if (sanity_check_result != 0) {
               cout << leaf->count << endl;
            }
            ensure(sanity_check_result == 0);
         }
         // -------------------------------------------------------------------------------------
         s16 pos = leaf->lowerBound<true>(key, key_length);
         if (pos != -1) {
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck_done();
            jumpmu_return OP_RESULT::OK;
         } else {
            leaf.recheck_done();
            raise(SIGTRAP);
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
                           function<void()>)
{
   jumpmuTry()
   {
      BTreePessimisticIterator<LATCH_FALLBACK_MODE::SHARED> iterator(*dynamic_cast<BTreeGeneric*>(this));
      auto ret = iterator.seek(Slice(start_key, key_length));
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         } else {
            if (iterator.next() != OP_RESULT::OK) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   jumpmuTry()
   {
      BTreePessimisticIterator<LATCH_FALLBACK_MODE::SHARED> iterator(*dynamic_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekForPrev(Slice(start_key, key_length));
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         } else {
            if (iterator.prev() != OP_RESULT::OK) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::insert(u8* key, u16 key_length, u8* value, u16 value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   volatile u32 mask = 1;
   volatile u32 local_restarts_counter = 0;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> c_guard;
         findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(c_guard, key, key_length);
         // -------------------------------------------------------------------------------------
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         if (c_x_guard->prepareInsert(key, key_length, value_length)) {
            c_x_guard->insert(key, key_length, value, value_length);
            if (FLAGS_wal) {
               auto wal_entry = c_x_guard.reserveWALEntry<WALInsert>(key_length + value_length);
               wal_entry->type = WAL_LOG_TYPE::WALInsert;
               wal_entry->key_length = key_length;
               wal_entry->value_length = value_length;
               std::memcpy(wal_entry->payload, key, key_length);
               std::memcpy(wal_entry->payload + key_length, value, value_length);
               wal_entry.submit();
            }
            jumpmu_return OP_RESULT::OK;
         }
         // -------------------------------------------------------------------------------------
         // Release lock
         c_guard = std::move(c_x_guard);
         c_guard.kill();
         // -------------------------------------------------------------------------------------
         trySplit(*c_guard.bf);
         // -------------------------------------------------------------------------------------
         jumpmu_continue;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_structural_change[dt_id]++;
         local_restarts_counter++;
      }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::updateSameSize(u8* key,
                                  u16 key_length,
                                  function<void(u8* payload, u16 payload_size)> callback,
                                  WALUpdateGenerator wal_update_generator)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         // -------------------------------------------------------------------------------------
         HybridPageGuard<BTreeNode> c_guard;
         findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(c_guard, key, key_length);
         u32 local_restarts_counter = c_guard.hasFacedContention();  // current implementation uses the mutex
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         s16 pos = c_x_guard->lowerBound<true>(key, key_length);
         assert(pos != -1);
         u16 payload_length = c_x_guard->getPayloadLength(pos);
         // -------------------------------------------------------------------------------------
         if (FLAGS_wal) {
            // if it is a secondary index, then we can not use updateSameSize
            assert(wal_update_generator.entry_size > 0);
            // -------------------------------------------------------------------------------------
            auto wal_entry = c_x_guard.reserveWALEntry<WALUpdate>(key_length + wal_update_generator.entry_size);
            wal_entry->type = WAL_LOG_TYPE::WALUpdate;
            wal_entry->key_length = key_length;
            std::memcpy(wal_entry->payload, key, key_length);
            wal_update_generator.before(c_x_guard->getPayload(pos), wal_entry->payload + key_length);
            // The actual update by the client
            callback(c_x_guard->getPayload(pos), payload_length);
            wal_update_generator.after(c_x_guard->getPayload(pos), wal_entry->payload + key_length);
            wal_entry.submit();
         } else {
            callback(c_x_guard->getPayload(pos), payload_length);
         }
         // -------------------------------------------------------------------------------------
         if (FLAGS_contention_split && local_restarts_counter > 0) {
            const u64 random_number = utils::RandomGenerator::getRandU64();
            if ((random_number & ((1ull << FLAGS_cm_update_on) - 1)) == 0) {
               s64 last_modified_pos = c_x_guard.bf()->header.contention_tracker.last_modified_pos;
               c_x_guard.bf()->header.contention_tracker.last_modified_pos = pos;
               c_x_guard.bf()->header.contention_tracker.restarts_counter += local_restarts_counter;
               c_x_guard.bf()->header.contention_tracker.access_counter++;
               if ((random_number & ((1ull << FLAGS_cm_period) - 1)) == 0) {
                  const u64 current_restarts_counter = c_x_guard.bf()->header.contention_tracker.restarts_counter;
                  const u64 current_access_counter = c_x_guard.bf()->header.contention_tracker.access_counter;
                  const u64 normalized_restarts = 100.0 * current_restarts_counter / current_access_counter;
                  c_x_guard.bf()->header.contention_tracker.restarts_counter = 0;
                  c_x_guard.bf()->header.contention_tracker.access_counter = 0;
                  // -------------------------------------------------------------------------------------
                  if (last_modified_pos != pos && normalized_restarts >= FLAGS_cm_slowpath_threshold && c_x_guard->count > 2) {
                     s16 split_pos = std::min<s16>(last_modified_pos, pos);
                     c_guard = std::move(c_x_guard);
                     c_guard.kill();
                     jumpmuTry()
                     {
                        trySplit(*c_guard.bf, split_pos);
                        WorkerCounters::myCounters().contention_split_succ_counter[dt_id]++;
                     }
                     jumpmuCatch() { WorkerCounters::myCounters().contention_split_fail_counter[dt_id]++; }
                  }
               }
            }
         } else {
            c_guard = std::move(c_x_guard);
         }
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_update_same_size[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::remove(u8* key, u16 key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> c_guard;
         findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(c_guard, key, key_length);
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         if (c_x_guard->remove(key, key_length)) {
            if (FLAGS_wal) {
               auto wal_entry = c_x_guard.reserveWALEntry<WALRemove>(key_length);
               wal_entry->type = WAL_LOG_TYPE::WALRemove;
               wal_entry->key_length = key_length;
               // TODO: copy value
               std::memcpy(wal_entry->payload, key, key_length);
               wal_entry.submit();
            }
         } else {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         if (c_x_guard->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
            c_guard = std::move(c_x_guard);
            c_guard.kill();
            jumpmuTry() { tryMerge(*c_guard.bf); }
            jumpmuCatch()
            {
               // nothing, it is fine not to merge
            }
         }
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_structural_change[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::countEntries()
{
   return BTreeGeneric::countEntries();
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::countPages()
{
   return BTreeGeneric::countPages();
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::getHeight()
{
   return BTreeGeneric::getHeight();
}
// -------------------------------------------------------------------------------------
void BTreeLL::undo(void*, const u8*, const u64)
{
   // TODO: undo for storage
}
// -------------------------------------------------------------------------------------
void BTreeLL::todo(void*, const u8*, const u64) {}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTreeLL::getMeta()
{
   DTRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
                                    .find_parent = findParent,
                                    .check_space_utilization = checkSpaceUtilization,
                                    .checkpoint = checkpoint,
                                    .undo = undo,
                                    .todo = todo};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
struct ParentSwipHandler BTreeLL::findParent(void* btree_object, BufferFrame& to_find)
{
   return BTreeGeneric::findParent(*dynamic_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), to_find);
}  // -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
