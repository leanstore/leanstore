// BTreeVI and BTreeVW are work in progress!
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
class BTreeVW : public BTreeLL
{
  public:
   struct __attribute__((packed)) Version {
      u64 tts : 56;
      u64 lsn : 56;
      u32 in_memory_offset;
      u8 worker_id : 8;
      u8 is_removed : 1;
      u8 is_final : 1;
      Version(u8 worker_id, u64 tts, u64 lsn, bool is_deleted, bool is_final, u32 in_memory_offset)
          : tts(tts), lsn(lsn), in_memory_offset(in_memory_offset), worker_id(worker_id), is_removed(is_deleted), is_final(is_final)
      {
      }
      void reset()
      {
         worker_id = 0;
         tts = 0;
         lsn = 0;
         is_removed = 0;
         is_final = 0;
         in_memory_offset = 0;
      }
   };
   static constexpr u64 VW_PAYLOAD_OFFSET = sizeof(Version);
   // -------------------------------------------------------------------------------------
   struct WALVWEntry : WALEntry {
      Version prev_version;
   };
   struct WALBeforeAfterImage : WALVWEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALInitPage : WALVWEntry {
      DTID dt_id;
   };
   struct WALAfterImage : WALVWEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALLogicalSplit : WALVWEntry {
      PID parent_pid = -1;
      PID left_pid = -1;
      PID right_pid = -1;
      s32 right_pos = -1;
   };
   struct WALInsert : WALVWEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   struct WALUpdate : WALVWEntry {
      u16 key_length;
      u8 payload[];
   };
   struct WALRemove : WALVWEntry {
      u16 key_length;
      u16 payload_length;
      u8 payload[];
   };
   struct TODOEntry {  // In-memory
      u16 key_length;
      u8 key[];
   };
   static_assert(sizeof(Version) == (20), "");
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   virtual OP_RESULT updateSameSizeInPlace(u8* key,
                                           u16 key_length,
                                           function<void(u8* value, u16 value_size)>,
                                           UpdateSameSizeInPlaceDescriptor&) override;
   virtual OP_RESULT remove(u8* key, u16 key_length) override;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                             function<void()>) override;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                              function<void()>) override;
   // -------------------------------------------------------------------------------------
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void* btree_object, const u8* wal_entry_ptr, const u64 version_worker_id, const u64 tx_id, const bool called_before);
   static void deserialize(void*, std::unordered_map<std::string, std::string>) {}      // TODO:
   static std::unordered_map<std::string, std::string> serialize(void*) { return {}; }  // TODO:
   static DTRegistry::DTMeta getMeta();

  private:
   bool reconstructTuple(u8* payload, u16& payload_length, u8 worker_id, u64 lsn, u32 in_memory_offset);
   static void applyDelta(u8* dst, u16 dst_size, const u8* delta, u16 delta_size);
   inline u8 myWorkerID() { return cr::Worker::my().worker_id; }
   inline u64 myTTS() { return cr::activeTX().TTS(); }
   inline u64 myWTTS() { return myWorkerID() | (myTTS() << 8); }
   inline bool isVisibleForMe(u8 worker_id, u64 tts) { return cr::Worker::my().isVisibleForMe(worker_id, tts); }
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
