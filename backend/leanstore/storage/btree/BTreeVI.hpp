// BTreeVI and BTreeVW are work in progress!
#pragma once
#include "BTreeLL.hpp"
#include "core/BTreeGenericIterator.hpp"
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
   using SN = u64;
   struct __attribute__((packed)) PrimaryVersion {
      u64 tts : 56;
      u8 worker_id : 8;
      u8 write_locked : 1;
      u8 is_removed : 1;
      u8 is_gc_scheduled : 1;
      // -------------------------------------------------------------------------------------
      SN next_sn = 0;
      // -------------------------------------------------------------------------------------
      PrimaryVersion(u8 worker_id, u64 tts) : tts(tts), worker_id(worker_id), write_locked(false), is_removed(false), is_gc_scheduled(false) {}
      bool isFinal() const { return next_sn == 0; }
      bool isWriteLocked() const { return write_locked; }
      void writeLock() { write_locked = true; }
      void unlock() { write_locked = false; }
   };
   struct __attribute__((packed)) SecondaryVersion {
      u8 worker_id : 8;
      u64 tts : 56;
      u8 is_removed : 1;
      u8 is_delta : 1;  // TODO: atm, always true
      SN next_sn;
      u8 is_skippable : 1;  // TODO: atm, not used
      SecondaryVersion(u8 worker_id, u64 tts, bool is_removed, bool is_delta, SN next_sn = 0)
          : worker_id(worker_id), tts(tts), is_removed(is_removed), is_delta(is_delta), next_sn(next_sn)
      {
      }
      bool isFinal() const { return next_sn == 0; }
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
   struct WALUpdateSSIP : WALEntry {
      u16 key_length;
      u64 delta_length;
      u8 before_worker_id;
      u8 after_worker_id;
      u64 before_tts;
      u64 after_tts;
      u8 payload[];
   };
   struct WALRemove : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 before_worker_id;
      u64 before_tts;
      u8 payload[];
   };
   // -------------------------------------------------------------------------------------
   struct TODOEntry {
      u16 key_length;
      SN sn;
      u8 key[];
   };
   // -------------------------------------------------------------------------------------
   OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   OP_RESULT updateSameSizeInPlace(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, UpdateSameSizeInPlaceDescriptor&) override;
   OP_RESULT remove(u8* key, u16 key_length) override;
   OP_RESULT scanAsc(u8* start_key,
                     u16 key_length,
                     function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                     function<void()>) override;
   OP_RESULT scanDesc(u8* start_key,
                      u16 key_length,
                      function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                      function<void()>) override;
   // -------------------------------------------------------------------------------------
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void* btree_object, const u8* wal_entry_ptr, const u64);
   static void deserialize(void*, std::unordered_map<std::string, std::string>) {}      // TODO:
   static std::unordered_map<std::string, std::string> serialize(void*) { return {}; }  // TODO:
   static DTRegistry::DTMeta getMeta();

  private:
   template <bool asc = true>
   void scan(u8* o_key, u16 o_key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
   {
      u64 counter = 0;
      volatile bool keep_scanning = true;
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
         MutableSlice s_key = iterator.mutableKeyInBuffer(o_key_length + sizeof(SN));
         std::memcpy(s_key.data(), o_key, o_key_length);
         setSN(s_key, 0);
         OP_RESULT ret;
         if (asc) {
            ret = iterator.seek(Slice(s_key.data(), s_key.length()));
         } else {
            ret = iterator.seekForPrev(Slice(s_key.data(), s_key.length()));
         }
         while (ret == OP_RESULT::OK) {
            iterator.assembleKey();
            Slice key = iterator.key();
            s_key = iterator.mutableKeyInBuffer();
            // -------------------------------------------------------------------------------------
            while (getSN(key) != 0) {
               if (asc) {
                  ret = iterator.next();
               } else {
                  ret = iterator.prev();
               }
               if (ret != OP_RESULT::OK) {
                  jumpmu_return;
               }
               iterator.assembleKey();
               key = iterator.key();
               s_key = iterator.mutableKeyInBuffer();
            }
            // -------------------------------------------------------------------------------------
            // costs 2K
            const u16 chain_length = std::get<1>(reconstructTuple(iterator, s_key, [&](Slice value) {
               keep_scanning = callback(s_key.data(), s_key.length() - sizeof(SN), value.data(), value.length());
               counter++;
            }));
            if (!keep_scanning) {
               jumpmu_return;
            }
            if (chain_length > 1) {
               setSN(s_key, 0);
               ret = iterator.seekExact(Slice(s_key.data(), s_key.length()));
               ensure(ret == OP_RESULT::OK);
            }
            // -------------------------------------------------------------------------------------
            if (asc) {
               ret = iterator.next();
            } else {
               ret = iterator.prev();
            }
         }
         jumpmu_return;
      }
      jumpmuCatch() { ensure(false); }
   }
   // -------------------------------------------------------------------------------------
   inline u8 myWorkerID() { return cr::Worker::my().worker_id; }
   inline u64 myTTS() { return cr::Worker::my().active_tx.tts; }
   inline u64 myWTTS() { return myWorkerID() | (myTTS() << 8); }
   inline bool isVisibleForMe(u8 worker_id, u64 tts) { return cr::Worker::my().isVisibleForMe(worker_id, tts); }
   inline bool isVisibleForMe(u64 wtts) { return cr::Worker::my().isVisibleForMe(wtts); }
   inline SwipType sizeToVT(u64 size) { return SwipType(reinterpret_cast<BufferFrame*>(size)); }
   // -------------------------------------------------------------------------------------
   template <typename T>
   inline SN getSN(T key)
   {
      return swap(*reinterpret_cast<const SN*>(key.data() + key.length() - sizeof(SN)));
   }
   inline void setSN(MutableSlice key, SN sn) { *reinterpret_cast<SN*>(key.data() + key.length() - sizeof(SN)) = swap(sn); }
   static void applyDelta(u8* dst, const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* src);
   inline std::tuple<OP_RESULT, u16> reconstructTuple(BTreeSharedIterator& iterator, MutableSlice key, std::function<void(Slice value)> callback)
   {
      Slice payload = iterator.value();
      assert(getSN(key) == 0);
      const PrimaryVersion& primary_version = *reinterpret_cast<const PrimaryVersion*>(payload.data() + payload.length() - sizeof(PrimaryVersion));
      if (isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
         if (primary_version.is_removed) {
            return {OP_RESULT::NOT_FOUND, 1};
         }
         callback(payload.substr(0, payload.length() - sizeof(PrimaryVersion)));
         return {OP_RESULT::OK, 1};
      } else {
         if (primary_version.isFinal()) {
            return {OP_RESULT::NOT_FOUND, 1};
         } else {
            return reconstructTupleSlowPath(iterator, key, callback);
         }
      }
   }
   std::tuple<OP_RESULT, u16> reconstructTupleSlowPath(BTreeSharedIterator& iterator, MutableSlice key, std::function<void(Slice value)> callback);
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
