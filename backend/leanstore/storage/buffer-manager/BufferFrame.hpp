#pragma once
#include "Swip.hpp"
#include "Units.hpp"
#include "leanstore/sync-primitives/Latch.hpp"
#include "leanstore/utils/simd.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cstring>
#include <set>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
const u64 PAGE_SIZE = 4 * 1024;
// -------------------------------------------------------------------------------------
struct BufferFrame {
   enum class STATE : u8 { FREE = 0, HOT = 1, LOADED = 2 };
   struct Header {
      WORKERID last_writer_worker_id = std::numeric_limits<u8>::max();  // for RFA
      LID last_written_plsn = 0;
      STATE state = STATE::FREE;  // INIT:
      std::atomic<bool> is_being_written_back = false;
      bool keep_in_memory = false;
      PID pid = 9999;         // INIT:
      HybridLatch latch = 0;  // INIT: // ATTENTION: NEVER DECREMENT
      // -------------------------------------------------------------------------------------
      BufferFrame* next_free_bf = nullptr;
      // -------------------------------------------------------------------------------------
      // Contention Split data structure
      struct ContentionTracker {
         u32 restarts_counter = 0;
         u32 access_counter = 0;
         s32 last_modified_pos = -1;
         void reset()
         {
            restarts_counter = 0;
            access_counter = 0;
            last_modified_pos = -1;
         }
      };
      ContentionTracker contention_tracker;
      // -------------------------------------------------------------------------------------
      struct OptimisticParentPointer {
         HybridLatch latch = 0;
         BufferFrame* parent_bf = nullptr;
         PID parent_pid;
         LID parent_plsn = 0;
         BufferFrame** swip_ptr = nullptr;
         s64 pos_in_parent = -1;
         void update(BufferFrame* new_parent_bf, PID new_parent_pid, LID new_parent_gsn, BufferFrame** new_swip_ptr, s64 new_pos_in_parent, Guard& bf_guard)
         {
            Guard guard(latch);
            guard.toOptimisticOrJump();
            bf_guard.recheck();
            if (parent_bf != new_parent_bf || parent_pid != new_parent_pid || parent_plsn != new_parent_gsn || swip_ptr != new_swip_ptr ||
                pos_in_parent != new_pos_in_parent) {
               guard.tryToExclusive();
               parent_bf = new_parent_bf;
               parent_pid = new_parent_pid;
               parent_plsn = new_parent_gsn;
               swip_ptr = new_swip_ptr;
               pos_in_parent = new_pos_in_parent;
               guard.unlock();
            }
         }
      };
      OptimisticParentPointer optimistic_parent_pointer;
      // -------------------------------------------------------------------------------------
      struct alignas(64) Tracker {
         alignas(64) static std::atomic<WATT_TIME> globalTrackerTime;
         alignas(64) static const u8 kw = 4, kr = 8;
         static const WATT_TIME security_distance = 1000;
         static constexpr float table_r[8][8] = {
             {0.2,8,7,6,5,4,3,2},
             {2,0.2,8,7,6,5,4,3},
             {3,2,0.2,8,7,6,5,4},
             {4,3,2,0.2,8,7,6,5},
             {5,4,3,2,0.2,8,7,6},
             {6,5,4,3,2,0.2,8,7},
             {7,6,5,4,3,2,0.2,8},
             {8,7,6,5,4,3,2,0.2},
         };
         static constexpr float table_w[4][8] = {
             {0.2,4,3,2,1,1,1,1},
             {2,0.2,4,3,1,1,1,1},
             {3,2,0.2,4,1,1,1,1},
             {4,3,2,0.2,1,1,1,1},
         };
         std::atomic<WATT_TIME> writesAndReads[kr+kw];
         std::atomic<u8> readPos, writePos;

         Tracker() { clear();}

         void clear(WATT_TIME now = globalTrackerTime.load()){
            readPos = 0;
            writePos = 0;
            for (u8 i=0; i<kr + kw; i++) {
               writesAndReads[i] = now - security_distance;
            }
         }
         void merge(Tracker& other, WATT_TIME now = globalTrackerTime.load()){
            readPos.store(addOther<false>(writesAndReads, other.writesAndReads, now), std::memory_order_release);
            writePos.store(addOther<true>(writesAndReads, other.writesAndReads, now), std::memory_order_release);
         }
         Tracker& operator=(const Tracker& other){
            if(this == &other){
               return *this;
            }
            // Exclusive locked -> memory_order relevant?
            readPos.store(other.readPos, std::memory_order_release);
            writePos.store(other.writePos, std::memory_order_release);
            for (u8 i=0; i<kr + kw; i++) {
               writesAndReads[i].store(other.writesAndReads[i], std::memory_order_release);
            }
            return *this;
         }
         void trackRead(WATT_TIME now = globalTrackerTime.load()){
            track<false>(now);
         }
         void trackWrite(WATT_TIME now = globalTrackerTime.load()){
            track<true>(now);
         }
         double getValue(WATT_TIME now = globalTrackerTime.load()){
            u32 reads_array[8] = {writesAndReads[kw],writesAndReads[kw+1],writesAndReads[kw+2],writesAndReads[kw+3],writesAndReads[kw+4],writesAndReads[kw+5],writesAndReads[kw+6],writesAndReads[kw+7]};
            u32 write_array[8] = {writesAndReads[0],writesAndReads[1], writesAndReads[2],writesAndReads[3],0,0,0,0};

            double readFreq = simd_getFreq<false>(reads_array, readPos, now);
            double writeFreq = simd_getFreq<true>(write_array, writePos, now);
            return readFreq + writeFreq * FLAGS_write_costs;
         }
        private:
         template <bool write>
         void track(WATT_TIME now = globalTrackerTime.load()){
            auto p = write? writePos.load() : readPos.load();
            if (now != writesAndReads[write ? p : p+kw]) {
               auto newPos = (p+1)%(write ? kw : kr);
               writesAndReads[write ? newPos : newPos + kw].store(now, std::memory_order_release);
               (write? writePos : readPos).store(newPos, std::memory_order_release);
            }
         }
         template<bool write>
         u8 addOther(std::atomic<WATT_TIME>* myArray, std::atomic<WATT_TIME>* otherArray, WATT_TIME now){
            std::set<WATT_TIME> local;
            u8 start = write? 0 : kw;
            u8 k = write? kw: kr;
            for (u8 i=0; i< k; i++){
               auto value = now - myArray[start + i].load();
               if(value < security_distance){
                  local.insert(value);
               }
            }
            for (u8 i=0; i< k; i++){
               auto value = now - otherArray[start + i].load();
               if(value < security_distance){
                  local.insert(value);
               }
            }
            // Reduce size to the maximum size
            while(local.size() > k){
               local.erase(std::prev(local.end()));
            }
            // Transfer Timestamps to my Array
            u8 kWalk = 0;
            for(u8 i=0; i < k; i++){
               if(!local.empty()) {
                  myArray[start + i].store(now - *std::prev(local.end()), std::memory_order_release);
                  local.erase(std::prev(local.end()));
                  kWalk = i;
               } else {
                  myArray[start + i].store(now - security_distance, std::memory_order_release);
               }
            }
            return kWalk;
         }
         template <bool write>
         float simd_getFreq(WATT_TIME* timestamps, unsigned pos, WATT_TIME now) {
            U8 a(timestamps);
            U8 nowV(now);
            F8 diff(nowV-a);
            F8 i(write? table_w[pos] : table_r[pos]);
            F8 div(i/diff);
            F8 one(1);
            F8 result(_mm256_min_ps(div,one));
            return maxV<write>(result);
         }
      };
      Tracker tracker = Tracker();
      u64 crc = 0;
   };
   struct alignas(512) Page {
      LID PLSN = 0;
      LID GSN = 0;
      DTID dt_id = 9999;                                                                               // INIT: datastructure id
      u64 magic_debugging_number;                                                                      // ATTENTION
      u8 dt[PAGE_SIZE - sizeof(PLSN) - sizeof(GSN) - sizeof(dt_id) - sizeof(magic_debugging_number)];  // Datastruture BE CAREFUL HERE !!!!!
      // -------------------------------------------------------------------------------------
      operator u8*() { return reinterpret_cast<u8*>(this); }
      // -------------------------------------------------------------------------------------
   };
   // -------------------------------------------------------------------------------------
   struct Header header;
   // -------------------------------------------------------------------------------------
   struct Page page;  // The persisted part
   // -------------------------------------------------------------------------------------
   bool operator==(const BufferFrame& other) { return this == &other; }
   // -------------------------------------------------------------------------------------
   inline bool isDirty() const { return page.PLSN != header.last_written_plsn; }
   inline bool isFree() const { return header.state == STATE::FREE; }
   // -------------------------------------------------------------------------------------
   // Pre: bf is exclusively locked
   void reset()
   {
      header.crc = 0;
      // -------------------------------------------------------------------------------------
      assert(!header.is_being_written_back);
      header.latch.assertExclusivelyLatched();
      header.last_writer_worker_id = std::numeric_limits<u8>::max();
      header.last_written_plsn = 0;
      header.state = STATE::FREE;  // INIT:
      header.is_being_written_back.store(false, std::memory_order_release);
      header.pid = 9999;
      header.next_free_bf = nullptr;
      header.contention_tracker.reset();
      header.keep_in_memory = false;
      header.tracker.clear();
      // std::memset(reinterpret_cast<u8*>(&page), 0, PAGE_SIZE);
   }
   // -------------------------------------------------------------------------------------
   BufferFrame() { header.latch->store(0ul); }
};
// -------------------------------------------------------------------------------------
static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(BufferFrame::Page::dt);
// -------------------------------------------------------------------------------------
static_assert(sizeof(BufferFrame::Page) == PAGE_SIZE, "");
// -------------------------------------------------------------------------------------
static_assert((sizeof(BufferFrame) - sizeof(BufferFrame::Page)) == 512, "");
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
