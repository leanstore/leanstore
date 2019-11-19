#pragma once
#include "Units.hpp"
#include "Swip.hpp"
#include "DTRegistry.hpp"
#include "BufferFrame.hpp"
#include "Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <sys/mman.h>
#include <cstring>
#include <queue>
#include <mutex>
#include <list>
#include <unordered_map>
#include <libaio.h>
#include <thread>
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
/*
 * Swizzle a page:
 * 1- bf_s_lock global bf_s_lock
 * 2- if it is in cooling stage:
 *    a- yes: bf_s_lock write (spin till you can), remove from stage, swizzle in
 *    b- no: set the state to IO, increment the counter, hold the mutex, p_read
 * 3- if it is in IOFlight:
 *    a- increment counter,
 */
class BufferManager {
   struct Stats {
      atomic<u64> swizzled_pages_counter = 0;
      atomic<u64> unswizzled_pages_counter = 0;
      atomic<u64> flushed_pages_counter = 0;
      void print();
      void reset();
   };
   struct CIOFrame {
      enum class State {
         READING,
         COOLING,
         NOT_LOADED
      };
      std::mutex mutex;
      std::list<BufferFrame*>::iterator fifo_itr;
      State state = State::NOT_LOADED;
      // -------------------------------------------------------------------------------------
      // Everything in CIOFrame is protected by global bf_s_lock except the following counter
      atomic<u64> readers_counter = 0;
   };
private:
   Config config;
   // -------------------------------------------------------------------------------------
   BufferFrame *bfs;
   // -------------------------------------------------------------------------------------
   int ssd_fd;
   // -------------------------------------------------------------------------------------
   std::mutex reservoir_mutex;
   // DRAM Pages
   atomic<u64> dram_free_bfs_counter = 0;
   std::vector<BufferFrame*> dram_free_bfs;
   // SSD Pages
   std::vector<PID> ssd_free_pages;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   std::mutex global_mutex;
   atomic<u64> cooling_bfs_counter = 0;
   std::list<BufferFrame*> cooling_fifo_queue;
   std::unordered_map<PID, CIOFrame> cooling_io_ht;
   // -------------------------------------------------------------------------------------
   // Threads managements
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   // -------------------------------------------------------------------------------------
   // Datastructures managements
   DTRegistry dt_registry;
   // -------------------------------------------------------------------------------------
   // Misc
   BufferFrame &randomBufferFrame();
   Stats stats;
   // -------------------------------------------------------------------------------------
   atomic<s64> phase_1_ms = 0, phase_2_ms = 0, phase_3_ms = 0;
public:
   BufferManager();
   BufferManager(Config config_snap);
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame &allocatePage();
   BufferFrame &resolveSwip(ReadGuard &swip_guard, Swip<BufferFrame> &swip_value);
   void deletePageWithBf(BufferFrame &bf);
   void initializeBfRoutine(BufferFrame &bf, PID pid);
   // -------------------------------------------------------------------------------------
   void flushDropAllPages();
   void stopBackgroundThreads();
   /*
    * Life cycle of a fix:
    * 1- Check if the pid is swizzled, if yes then store the BufferFrame address temporarily
    * 2- if not, then posix_check if it exists in cooling stage queue, yes? remove it from the queue and return
    * the buffer frame
    * 3- in anycase, posix_check if the threshold is exceeded, yes ? unswizzle a random BufferFrame
    * (or its children if needed) then add it to the cooling stage.
    */
   // -------------------------------------------------------------------------------------
   void readPageSync(PID pid, u8 *destination);
   void fDataSync();
   // -------------------------------------------------------------------------------------
   void registerDatastructureType(DTType type, DTRegistry::DTMeta dt_meta);
   DTID registerDatastructureInstance(DTType type,  void *root_object);
   // -------------------------------------------------------------------------------------
   void clearSSD();
   void restore();
   void persist();
   // -------------------------------------------------------------------------------------
   u64 consumedPages();
};
// -------------------------------------------------------------------------------------
class BMC {
public:
   static BufferManager* global_bf;
};
}
}
// -------------------------------------------------------------------------------------
