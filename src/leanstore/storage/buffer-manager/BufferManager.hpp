#pragma once
#include "Units.hpp"
#include "Swip.hpp"
#include "BufferFrame.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <sys/mman.h>
#include <cstring>
#include <queue>
#include <mutex>
#include <list>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
class BufferManager {
   struct CIOFrame {
      enum class State {
         READING,
         COOLING
      };
      std::mutex mutex;
      bool loaded = false;
      BufferFrame *bf = nullptr;
      std::list<BufferFrame*>::iterator fifo_itr;
      State state;
   };
private:
   u8 *dram;
   int ssd_fd;
   u32 buffer_frame_size;
   // -------------------------------------------------------------------------------------
   // DRAM Pages
   atomic<u64> dram_free_bfs_counter = 0;
   std::queue<BufferFrame*> dram_free_bfs;
   std::queue<BufferFrame*> dram_used_bfs;
   // -------------------------------------------------------------------------------------
   // SSD Pages
   std::queue<u64> ssd_free_pages;
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   std::mutex global_mutex;
   std::list<BufferFrame*> cooling_fifo_queue;
   std::unordered_map<PID, CIOFrame> cooling_io_ht;
   // -------------------------------------------------------------------------------------
public:
   BufferManager();
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame *getLoadedBF(PID pid);
   void checkCoolingThreshold();
   BufferFrame &accquirePage();
   BufferFrame &fixPage(Swip &swizzle);
   void unfixPage(Swip &swizzle);
   /*
    * Life cycle of a fix:
    * 1- Check if the pid is swizzled, if yes then store the BufferFrame address temporarily
    * 2- if not, then check if it exists in cooling stage queue, yes? remove it from the queue and return
    * the buffer frame
    * 3- in anycase, check if the threshold is exceeded, yes ? unswizzle a random BufferFrame
    * (or its children if needed) then add it to the cooling stage.
    */
   // -------------------------------------------------------------------------------------
   void readPage(PID pid, u8 *destination);
   void writePage(u8 *src, PID pid);
   void flush();
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class BMC {
public:
   static unique_ptr<BufferManager> global_bf;
   static void start();
};
}
// -------------------------------------------------------------------------------------
