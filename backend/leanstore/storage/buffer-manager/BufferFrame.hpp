#pragma once
#include "Units.hpp"
#include "Swip.hpp"
#include "leanstore/sync-primitives/Latch.hpp"
#include "leanstore/utils/simd.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cstring>
#include <vector>
#include <set>
#include <unordered_map>
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
   static std::atomic<WATT_TIME> globalTrackerTime;
   struct Header {
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
      static const u8 kr = 8, kw = 4;

      struct alignas(64) Tracker {
         std::atomic<WATT_TIME> reads[kr], writes[kw];
         std::atomic<u8> readPos, writePos;

         static constexpr float table_8[8][8] = {
             {1,8,7,6,5,4,3,2},
             {2,1,8,7,6,5,4,3},
             {3,2,1,8,7,6,5,4},
             {4,3,2,1,8,7,6,5},
             {5,4,3,2,1,8,7,6},
             {6,5,4,3,2,1,8,7},
             {7,6,5,4,3,2,1,8},
             {8,7,6,5,4,3,2,1},
         };

         static constexpr float table_4[4][8] = {
             {1,4,3,2,1,1,1,1},
             {2,1,4,3,1,1,1,1},
             {3,2,1,4,1,1,1,1},
             {4,3,2,1,1,1,1,1},
         };
         float simd_getFreq(WATT_TIME* timestamps, unsigned pos, WATT_TIME now, bool use8) {
            U8 a(timestamps);
            U8 nowV(now);
            F8 diff(nowV-a);
            F8 i(use8? table_8[pos] : table_4[pos]);
            F8 div(i/diff);
            F8 one(1);
            F8 result(_mm256_min_ps(div,one));
            return maxV(result, use8);
         }

         Tracker() { clear();}

         void trackRead(WATT_TIME ts = globalTrackerTime.load()) {
            auto p = readPos.load();
            if (ts != reads[p]) {
               auto newPos = (p+1)%kr;
               reads[newPos].store(ts, std::memory_order_release);
               readPos.store(newPos, std::memory_order_release);
            }
         }

         void trackWrite(WATT_TIME ts = globalTrackerTime.load()) {
            auto p = writePos.load();
            if (ts != writes[p]) {
               auto newPos = (p+1) % kw;
               writes[newPos].store(ts, std::memory_order_release);
               writePos.store(newPos, std::memory_order_release);
            }
         }
         void clear(){
            readPos = 0;
            writePos = 0;
            for (u8 i=0; i<kr || i < kw; i++) {
               if (i < kr)
                  reads[i] = 0;
               if (i < kw)
                  writes[i] = 0;
            }
         }
         // Get Max of all accesses
         void merge(Tracker& other){
            readPos.store(addOther<kr>(reads, other.reads), std::memory_order_release);
            writePos.store(addOther<kw>(writes, other.writes), std::memory_order_release);
         }
         template<u8 k>
         u8 addOther(std::atomic<WATT_TIME>* myArray, std::atomic<WATT_TIME>* otherArray){
            std::set<WATT_TIME> local;
            for (u8 i=0; i< k; i++){
               local.insert(myArray[i].load());
            }
            for (u8 i=0; i< k; i++){
               local.insert(otherArray[i].load());
            }
            while(local.size() > k){
               local.erase(local.begin());
            }
            u8 kRet = 0;
            for(u8 kWalk =0; kWalk < k; kWalk++){
               if(!local.empty()) {
                  myArray[kWalk].store(*local.begin(), std::memory_order_release);
                  local.erase(local.begin());
                  kRet = kWalk;
               } else {
                  myArray[kWalk].store(0, std::memory_order_release);
               }
            }
            return kRet;

         }
         Tracker& operator=(const Tracker& other){
            if(this == &other){
               return *this;
            }
            // Exclusive locked -> memory_order relevant?
            readPos.store(other.readPos, std::memory_order_release);
            writePos.store(other.writePos, std::memory_order_release);
            for (u8 i=0; i<kr || i < kw; i++) {
               if (i < kr)
                  reads[i].store(other.reads[i], std::memory_order_release);
               if (i < kw)
                  writes[i].store(other.writes[i], std::memory_order_release);
            }
            return *this;
         }
         double getValue(WATT_TIME now = globalTrackerTime.load()){
            u32 reads_array[8] = {reads[0],reads[1], reads[2],reads[3],reads[4],reads[5],reads[6],reads[7]};
            u32 write_array[8] = {writes[0],writes[1], writes[2],writes[3],0,0,0,0};

            double readFreq = simd_getFreq(reads_array, readPos, now, true);
            double writeFreq = simd_getFreq(write_array, writePos, now, false);
            return readFreq + writeFreq * FLAGS_write_costs;
         }
      };
      struct Tracker_store {
         u8 readPos, writePos;
         WATT_TIME reads[kr], writes[kw];
         Tracker_store():readPos(0), writePos(0){
            for (u8 i=0; i<kr || i < kw; i++) {
               if (i < kr)
                  reads[i] = 0;
               if (i < kw)
                  writes[i] = 0;
            }
         }
         Tracker_store(const Tracker& track): readPos(track.readPos.load()), writePos(track.writePos.load()){
            for (u8 i=0; i<kr || i < kw; i++) {
               if (i < kr)
                  reads[i] = track.reads[i];
               if (i < kw)
                  writes[i] = track.writes[i];
            }
         }
         void toTracker(Tracker& track){
            track.readPos.store(readPos, std::memory_order_release);
            track.writePos.store(writePos, std::memory_order_release);
            for (u8 i=0; i<kr || i < kw; i++) {
               if (i < kr)
                  track.reads[i].store(reads[i], std::memory_order_release);
               if (i < kw)
                  track.writes[i].store(writes[i], std::memory_order_release);
            }
         }
      };
      struct WATT_LOG{
         WATT_LOG(PID page = 1000000){
             checkSizes(page, true);
         };
        private:
         std::vector<Tracker_store> pid_trackers;
         PID currSize = 0;
        public:
         void store(PID page, const Tracker& from){
            checkSizes(page);
            pid_trackers[page] = Tracker_store(from);
         }

         void load(PID page, Tracker& into){
            checkSizes(page);
            Tracker_store store = pid_trackers[page];
            store.toTracker(into);
         }
         void checkSizes(PID page, [[maybe_unused]] bool init = false){
            if(currSize <= page){
               cout << "Resizing to " << page << endl;
               if(!init){
                  assert(init);
                  std::system_error();
               }
               currSize = page+1;
               pid_trackers.resize(currSize);
            }
         }
      };
      // TODO: for logging
      u64 lastWrittenGSN = 0;
      STATE state = STATE::FREE;  // INIT:
      bool isInWriteBuffer = false;
      bool keep_in_memory = false;
      PID pid = 9999;         // INIT:
      HybridLatch latch = 0;  // INIT: // ATTENTION: NEVER DECREMENT
      Tracker tracker = Tracker();
      // -------------------------------------------------------------------------------------
      BufferFrame* next_free_bf = nullptr;
      ContentionTracker contention_tracker;
      std::shared_mutex meta_data_in_shared_mode_mutex;
      // -------------------------------------------------------------------------------------
      u64 debug;
     public:
      static WATT_LOG watt_backlog;
   };
   struct alignas(512) Page {
      u64 GSN = 0;
      DTID dt_id = 9999;                                                                // INIT: datastructure id
      u64 magic_debugging_number;                                                       // ATTENTION
      u8 dt[PAGE_SIZE - sizeof(GSN) - sizeof(dt_id) - sizeof(magic_debugging_number)];  // Datastruture BE CAREFUL HERE !!!!!
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
   inline bool isDirty() const { return header.lastWrittenGSN != page.GSN; }
   inline bool isFree() const { return header.state == STATE::FREE; }
   // -------------------------------------------------------------------------------------
   // Pre: bf is exclusively locked
   void reset()
   {
      header.debug = header.pid;
      // -------------------------------------------------------------------------------------
      assert(!header.isInWriteBuffer);
      header.latch.assertExclusivelyLatched();
      header.lastWrittenGSN = 0;
      header.state = STATE::FREE;  // INIT:
      header.isInWriteBuffer = false;
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
