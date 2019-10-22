#pragma once
#include "Units.hpp"
#include "Swizzle.hpp"
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
   struct IOFrame {
      std::mutex mutex;
      bool loaded = false;
      BufferFrame *bf = nullptr;
   };
private:
   u8 *dram;
   int ssd_fd;
   u32 buffer_frame_size;
   // -------------------------------------------------------------------------------------
   // DRAM Pages
   std::queue<BufferFrame*> dram_free_bfs;
   std::queue<BufferFrame*> dram_used_bfs;
   // -------------------------------------------------------------------------------------
   // SSD Pages
   std::mutex ssd_lists_mutex;
   std::queue<u64> ssd_free_pages;
   std::queue<u64> ssd_used_pages;
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   std::mutex global_mutex;
   // Cooling stage section
   std::list<BufferFrame*> cooling_queue;
   std::unordered_map<PID, std::list<BufferFrame*>::iterator> cooling_ht;
   // -------------------------------------------------------------------------------------
   // InFlight IO
   std::unordered_map<PID, IOFrame> inflight_io;
   // -------------------------------------------------------------------------------------
public:
   BufferManager();
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame *getLoadedBF(PID pid);
   void checkCoolingThreshold();
   BufferFrame &accquirePage();
   BufferFrame &fixPage(Swizzle &swizzle);
   void unfixPage(Swizzle &swizzle);
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
}
// -------------------------------------------------------------------------------------
