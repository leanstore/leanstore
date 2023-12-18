#pragma once
#include "Swip.hpp"
#include "Units.hpp"
#include "leanstore/sync-primitives/PlainGuard.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <optional>
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
   enum class STATE : u8 { FREE = 0, HOT = 1, COOL = 2, LOADED = 3, IOCOLD = 4, IOCOLDDONE = 5, IOPOPPED = 6, IOLOST = 7, IOLOST2 = 8, COUNT = 9 /*keep as max*/};
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
      // TODO: for logging
      u64 lastWrittenGSN = 0;
      STATE state = STATE::FREE;  // INIT:
      bool isWB = false;
      bool keep_in_memory = false;
      bool newPage = false;
      PID pid = 9999;         // INIT:
      HybridLatch latch = 0;  // INIT: // ATTENTION: NEVER DECREMENT
      // -------------------------------------------------------------------------------------
      BufferFrame* next_free_bf = nullptr;
      ContentionTracker contention_tracker;
      // -------------------------------------------------------------------------------------
      struct OptimisticParentPointer {
         struct Parent {
            LID last_swip_invalidation_version = 0;
         } parent;
         struct Child {
            BufferFrame* parent_bf = nullptr;
            PID parent_pid;
            u64 parent_bf_version_on_update = 0;
            BufferFrame** swip_ptr = nullptr;
            s64 pos_in_parent = -1;
            bool updateRequired(BufferFrame* new_parent_bf, PID new_parent_pid, BufferFrame** new_swip_ptr, s64 new_pos_in_parent, u64 last_swip_invalidation_version)
            {
               return parent_bf_version_on_update < last_swip_invalidation_version ||  parent_bf != new_parent_bf || parent_pid != new_parent_pid || swip_ptr != new_swip_ptr ||
                  pos_in_parent != new_pos_in_parent;
            }
            void update(BufferFrame* new_parent_bf, PID new_parent_pid, BufferFrame** new_swip_ptr, s64 new_pos_in_parent, u64 new_parent_bf_version_on_update, std::string loc)
            {
               //std::stringstream ss;
               //ss << loc << " update: this: " << this << " p_bf: " << new_parent_bf << " p_pid: " << new_parent_pid << " pos: " << new_pos_in_parent << " v_on_upd: " << parent_bf_version_on_update << endl;
               //cout << ss.str();
               parent_bf = new_parent_bf;
               parent_pid = new_parent_pid;
               swip_ptr = new_swip_ptr;
               pos_in_parent = new_pos_in_parent;
               parent_bf_version_on_update = new_parent_bf_version_on_update;
               ensure(swip_ptr);
            }
            // returns empty if the opp version check fails
            std::optional<Guard> checkNoJump(Guard& c_guard ) {
               if (c_guard.tryRecheck() && parent_bf != nullptr) {
                  Guard p_guard(parent_bf->header.latch);
                  if (p_guard.tryToOptimistic() && parent_bf->header.pid == parent_pid) {
                     assert(parent_bf_version_on_update <= parent_bf->header.latch.version);
                     // The last_swip_invalidation_version must always be updated when there is an organizational change in an (inner) node.
                     // i.e. the position of swips changes (like at an insert in an inner node which only happens when a leaf/inner node is split.)
                     // When the page is only updated to swizzle swips, this doesn't matter. Swips and positions are still valid.
                     // opp data is valid if the version when opp data was updated happened after its last invalidation
                     if (parent_bf->header.optimistic_parent_pointer.parent.last_swip_invalidation_version <= parent_bf_version_on_update) {
                        ensure(swip_ptr);
                        return std::move(p_guard);
                     }
                  }
               }
               return {};
            }
            // returns empty if the opp version check fails
            // jumps if exclusively locked
            std::optional<Guard> check(int dt_id) {
               if (parent_bf != nullptr) {
                  Guard p_guard(parent_bf->header.latch);
                  p_guard.toOptimisticOrJump();
                  if (parent_bf->header.pid == parent_pid) {
                     assert(parent_bf_version_on_update <= parent_bf->header.latch.version);
                     // see comment above in checkNoJump
                     if (parent_bf->header.optimistic_parent_pointer.parent.last_swip_invalidation_version <= parent_bf_version_on_update) {
                        ensure(swip_ptr);
                        return std::move(p_guard);
                     } else {
                        COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent_dbg2[dt_id]++; }
                     }
                  } else {
                     COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent_dbg[dt_id]++; }
                  }
               }
               return {};
            }
         } child;
      };
      OptimisticParentPointer optimistic_parent_pointer;
      // -------------------------------------------------------------------------------------
      u64 debug;
   };
   struct alignas(512) Page {
      u64 GSN = 0;
      DTID dt_id = 9999;                                                                // INIT: datastructure id
      u64 magic_debugging_number;                                                       // ATTENTION
      u8 dt[PAGE_SIZE - sizeof(GSN) - sizeof(dt_id) - 2*sizeof(magic_debugging_number)];  // Datastruture BE CAREFUL HERE !!!!!
      // -------------------------------------------------------------------------------------
      u64 magic_debugging_number_end;                                                       // ATTENTION
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
   // -------------------------------------------------------------------------------------
   // Pre: bf is exclusively locked
   void reset()
   {
      //header.debug = header.pid;
      // -------------------------------------------------------------------------------------
      assert(!header.isWB);
      header.latch.assertExclusivelyLatched();
      //header.lastWrittenGSN = 0;
      header.state = STATE::FREE;  // INIT:
      header.isWB = false;
      //header.pid = 9999;
      //header.next_free_bf = nullptr;
      header.contention_tracker.reset();
      header.optimistic_parent_pointer.parent.last_swip_invalidation_version = 9e18l;
      header.optimistic_parent_pointer.child.parent_bf = nullptr;
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
