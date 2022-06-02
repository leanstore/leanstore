#pragma once
#include "Units.hpp"
#include "Swip.hpp"
#include "leanstore/sync-primitives/Latch.hpp"
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
      struct Tracker {
         std::atomic<u8> readPos;
         std::atomic<u8> writePos;
         std::atomic<WATT_TIME> reads[kr];
         std::atomic<WATT_TIME> writes[kw];
         std::atomic<WATT_TIME> last_track;

         Tracker() { clear();}

         void trackRead() {
            auto p = readPos.load();
            auto ts = globalTrackerTime.load();
            if (ts != reads[p]) {
               auto newPos = (p+1)%kr;
               reads[newPos].store(ts, std::memory_order_release);
               readPos.store(newPos, std::memory_order_release);
               last_track.store(ts, std::memory_order_release);
            }
         }

         void trackWrite() {
            auto p = writePos.load();
            auto ts = last_track.load();
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
            last_track.store(other.last_track, std::memory_order_release);
            for (u8 i=0; i<kr || i < kw; i++) {
               if (i < kr)
                  reads[i].store(other.reads[i], std::memory_order_release);
               if (i < kw)
                  writes[i].store(other.writes[i], std::memory_order_release);
            }
            return *this;
         }
         double getValue(WATT_TIME now = globalTrackerTime.load()){
            double readFreq = getFrequency<kr>(reads, readPos.load(), now);
            double writeFreq = getFrequency<kr>(writes, writePos.load(), now);
            return readFreq + writeFreq * FLAGS_write_costs;
         }
        private:
         template<u8 k>
         double getFrequency(std::atomic<WATT_TIME>* array, u8 curr, WATT_TIME now){
            long value = 10;
            WATT_TIME best_age = 100000;
            long best_value = 0;
            for(s8 pos = curr; pos >= 0 && array[pos] != 0;  pos--){
               WATT_TIME age = now - array[pos] + 1;
               long left = value*best_age;
               long right = best_value * age;
               if(left> right){
                  best_value = value;
                  best_age = age;
               }
               value+=10;
            }
            for(s8 pos = k-1; pos > curr && array[pos] != 0; pos--){
               WATT_TIME age = now - array[pos];
               long left = value*best_age + 1;
               long right = best_value * age;
               if(left> right){
                  best_value = value;
                  best_age = age;
               }
               value+=10;
            }
            if(best_age ==0){
               best_age++;
            }
            return best_value * 1.0 /best_age;
         }
      };
      struct Tracker_store {
         u8 readPos, writePos;
         WATT_TIME reads[kr], writes[kw], last_track;
         Tracker_store():readPos(0), writePos(0), last_track(0){
            for (u8 i=0; i<kr || i < kw; i++) {
               if (i < kr)
                  reads[i] = 0;
               if (i < kw)
                  writes[i] = 0;
            }
         }
         Tracker_store(const Tracker& track): readPos(track.readPos.load()), writePos(track.writePos.load()), last_track(track.last_track.load()){
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
            track.last_track.store(last_track, std::memory_order_release);
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
