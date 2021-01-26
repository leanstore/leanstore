#pragma once
#include "BTreeLL.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
class BTreeVI : public BTreeLL
{
  public:
   struct __attribute__((packed)) PrimaryVersion {
      u64 tts : 56;
      u8 worker_id : 8;
      u64 next_version : 64;
      u8 is_removed : 1;
      u8 is_final : 1;
      PrimaryVersion(u8 worker_id, u64 tts, u64 next_version, bool is_removed, bool is_final)
          : tts(tts), worker_id(worker_id), next_version(next_version), is_removed(is_removed), is_final(is_final)
      {
      }
      void reset()
      {
         worker_id = 0;
         tts = 0;
         next_version = 0;
         is_removed = 0;
         is_final = 0;
      }
   };
   struct __attribute__((packed)) SecondaryVersion {
      u64 tts : 56;
      u8 worker_id : 8;
      u8 is_removed : 1;
      u8 is_delta : 1;      // TODO: atm, always true
      u8 is_skippable : 1;  // TODO: atm, not used
      SecondaryVersion(u8 worker_id, u64 tts, bool is_removed, bool is_delta)
          : tts(tts), worker_id(worker_id), is_removed(is_removed), is_delta(is_delta)
      {
      }
   };
   struct WALBeforeAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALInitPage : WALEntry {
      DTID dt_id;
   };
   struct WALAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALLogicalSplit : WALEntry {
      PID parent_pid = -1;
      PID left_pid = -1;
      PID right_pid = -1;
      s32 right_pos = -1;
   };
   struct WALInsert : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   struct WALUpdate : WALEntry {
      u16 key_length;
      u64 before_image_seq;
      u8 payload[];
   };
   struct WALRemove : WALEntry {
      u16 key_length;
      u16 value_length;
      u64 before_image_seq;
      u8 payload[];
   };
   // -------------------------------------------------------------------------------------
   OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0}) override;
   OP_RESULT remove(u8* key, u16 key_length) override;
   OP_RESULT scanAsc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>, function<void()>) override;
   OP_RESULT scanDesc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>, function<void()>) override;
   // -------------------------------------------------------------------------------------
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void*, const u8*, const u64);
   static DTRegistry::DTMeta getMeta();

  private:
   void iterateDesc(u8* start_key, u16 key_length, function<bool(HybridPageGuard<BTreeNode>& guard, s16 pos)> callback);
   inline u8 myWorkerID() { return cr::Worker::my().worker_id; }
   inline u64 myTTS() { return cr::Worker::my().active_tx.tts; }
   inline u64 myWTTS() { return myWorkerID() | (myTTS() << 8); }
   inline bool isVisibleForMe(u8 worker_id, u64 tts) { return cr::Worker::my().isVisibleForMe(worker_id, tts); }
   inline bool isVisibleForMe(u64 wtts) { return cr::Worker::my().isVisibleForMe(wtts); }
   inline SwipType sizeToVT(u64 size) { return SwipType(reinterpret_cast<BufferFrame*>(size)); }
   // -------------------------------------------------------------------------------------
   static void applyDelta(u8* dst, u8* delta, u16 delta_size);
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
