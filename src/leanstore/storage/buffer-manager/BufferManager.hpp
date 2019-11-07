#pragma once
#include "Units.hpp"
#include "Swip.hpp"
#include "DTRegistry.hpp"
#include "BufferFrame.hpp"
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
   };
   struct CIOFrame {
      enum class State {
         READING,
         COOLING,
         NOT_LOADED
      };
      std::mutex mutex;
      bool loaded = false;
      BufferFrame *bf = nullptr;
      std::list<BufferFrame*>::iterator fifo_itr;
      State state = State::NOT_LOADED;
      // -------------------------------------------------------------------------------------
      // Everything in CIOFrame is protected by global bf_s_lock except the following counter
      atomic<u64> readers_counter = 0;
   };
private:
   BufferFrame *bfs;
   // -------------------------------------------------------------------------------------
   int ssd_fd;
   io_context_t ssd_aio_context;
   std::mutex ssd_aio_mutex;
   std::list<u64> write_buffer_free_slots;
   std::unique_ptr<u8[]> write_buffer;
   std::unordered_map<uint32_t, std::tuple<u64, BufferFrame*>> ssd_aio_ht;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   std::mutex reservoir_mutex;
   // DRAM Pages
   atomic<u64> dram_free_bfs_counter = 0;
   std::queue<BufferFrame*> dram_free_bfs;
   // SSD Pages
   std::queue<u64> ssd_free_pages;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   std::mutex global_mutex;
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
public:
   BufferManager();
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame &allocatePage();

   BufferFrame &resolveSwip(SharedGuard &swip_guard, Swip<BufferFrame> &swip_value);

   void stopBackgroundThreads();
   /*
    * Life cycle of a fix:
    * 1- Check if the pid is swizzled, if yes then store the BufferFrame address temporarily
    * 2- if not, then check if it exists in cooling stage queue, yes? remove it from the queue and return
    * the buffer frame
    * 3- in anycase, check if the threshold is exceeded, yes ? unswizzle a random BufferFrame
    * (or its children if needed) then add it to the cooling stage.
    */
   // -------------------------------------------------------------------------------------
   void readPageSync(PID pid, u8 *destination);
   void writePageAsync(BufferFrame &bf);
   void flush();
   // -------------------------------------------------------------------------------------
   void registerDatastructureType(DTType type, DTRegistry::CallbackFunctions callback_functions);
   void registerDatastructureInstance(DTID dtid, DTType type,  void *root_object);
};
// -------------------------------------------------------------------------------------
class BMC {
public:
   static unique_ptr<BufferManager> global_bf;
   static void start();
};
}
// -------------------------------------------------------------------------------------
