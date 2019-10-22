#include "BufferManager.hpp"
#include "BufferFrame.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <unistd.h>
#include <tbb/tbb_thread.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(dram_pages, 1024, "");
DEFINE_uint32(ssd_pages, 1024, "");
DEFINE_uint32(page_size, 16 * 1024, "");
DEFINE_string(ssd_path, "/tmp/leanstore", "");
DEFINE_bool(ssd_truncate, true, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(cooling_threshold, 10, "Start cooling pages when 100-x% are free");
DEFINE_uint32(background_write_sleep, 10, "ms");
// -------------------------------------------------------------------------------------
namespace leanstore {
BufferManager::BufferManager()
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   buffer_frame_size = FLAGS_page_size + sizeof(BufferFrame);
   const u32 dram_total_size = buffer_frame_size * FLAGS_dram_pages;
   dram = reinterpret_cast<u8 *>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
   madvise(dram, dram_total_size, MADV_HUGEPAGE);
   memset(dram, 0, dram_total_size);
   // -------------------------------------------------------------------------------------
   /// Init SSD pool
   const u32 ssd_total_size = FLAGS_ssd_pages * FLAGS_page_size;
   int flags = O_RDWR | O_DIRECT | O_CREAT;
   if ( FLAGS_ssd_truncate ) {
      flags |= O_TRUNC;
   }
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   ftruncate(ssd_fd, ssd_total_size);
   check(ssd_fd > -1);
   if ( fcntl(ssd_fd, F_GETFL) == -1 ) {
      throw Generic_Exception("Can not initialize SSD storage: " + FLAGS_ssd_path);
   }
   // -------------------------------------------------------------------------------------
   for ( u64 pid = 0; pid < FLAGS_dram_pages; pid++ ) {
      dram_free_bfs.push(new(dram + (pid * buffer_frame_size)) BufferFrame(pid));
      inflight_io.emplace(std::piecewise_construct, std::forward_as_tuple(pid), std::forward_as_tuple());
   }
   for ( u64 pid = 0; pid < FLAGS_ssd_pages; pid++ ) {
      ssd_free_pages.push(pid);
   }
   // -------------------------------------------------------------------------------------
   std::srand(std::time(nullptr));
   tbb::tbb_thread myThread([]() {
      while ( true ) {
         // TODO: take a bf from the cooling stage, flush it if dirty and clear the dirty flag
         // does not imply removing it form the FIFO queue
         usleep(FLAGS_background_write_sleep);
      }
   });
   // TODO: Spawn a background writer thread
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   u32 dram_page_size = FLAGS_page_size + sizeof(BufferFrame);
   const u32 dram_total_size = dram_page_size * FLAGS_dram_pages;
   munmap(dram, dram_total_size);
   close(ssd_fd);
   ssd_fd = -1;
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
BufferFrame *BufferManager::getLoadedBF(PID pid)
{
   return reinterpret_cast<BufferFrame *>(dram + (pid * buffer_frame_size));
}
// -------------------------------------------------------------------------------------
void BufferManager::checkCoolingThreshold()
{
   /*
    * Plan:
    */
   check_again:
   // -------------------------------------------------------------------------------------
   // Check if we are running out of free pages
   if ((dram_free_bfs.size() * 100.0 / FLAGS_dram_pages) <= FLAGS_cooling_threshold ) {
      //TODO: pick a page, unswizzle it and move it to cooling stage
      PID cool_pid = std::rand() % dram_used_bfs.size();
      BufferFrame *cool_bf = getLoadedBF(cool_pid);
      // Make sure the BF is hot by checking whether its parent swizzle is swizzled
      while ( !cool_bf->parent_pointer->isSwizzled()) {
         cool_pid = std::rand() % dram_used_bfs.size();
         cool_bf = getLoadedBF(cool_pid);
      }
      bool all_children_unswizzled = true;
      for ( auto &swizzle: cool_bf->callback_function(cool_bf->payload)) {
         if ( swizzle->isSwizzled()) {
            all_children_unswizzled = false;
            break;
         }
      }
      if ( all_children_unswizzled ) {
         // TODO
      } else {
         // TODO: unswizzle a child
         goto check_again;
      }
   }
}
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::accquirePage()
{
   checkCoolingThreshold();
   global_mutex.lock();
   auto free_bf = dram_free_bfs.front();
   dram_free_bfs.pop();
   dram_used_bfs.push(free_bf);
   global_mutex.unlock();
   return *free_bf;
}
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::fixPage(Swizzle &swizzle)
{
   //TODO: wrong, fix it not equal to allocate !!!
   fix_again:
   // -------------------------------------------------------------------------------------
   // Check if we are running out of free pages
   checkCoolingThreshold();
   // -------------------------------------------------------------------------------------
   // Now, we can talk about fixing
   if ( swizzle.isSwizzled()) {
      return swizzle.getBufferFrame();
   } else {
      BufferFrame *return_bf = nullptr;
      global_mutex.lock();
      if (swizzle.isSwizzled()) { // maybe another thread has already fixed it
         return swizzle.getBufferFrame();
      }
      IOFrame &IOFrame = inflight_io.find(swizzle.asInteger())->second;
      IOFrame.mutex.lock();
      global_mutex.unlock();
      if ( IOFrame.loaded ) {
         IOFrame.mutex.unlock();
         return_bf = IOFrame.bf;
      } else {
         auto free_bf = dram_free_bfs.front();
         dram_free_bfs.pop();
         dram_used_bfs.push(free_bf);
         IOFrame.bf = free_bf;
         readPage(swizzle.asInteger(),free_bf->payload);
         IOFrame.loaded = true;
         IOFrame.mutex.unlock();
         return_bf = free_bf;
      }
      swizzle.swizzle(return_bf);
      return *return_bf;
   }
}

// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPage(u64 pid, u8 *destination)
{
   s64 read_bytes = pread(ssd_fd, destination, FLAGS_page_size, pid * FLAGS_page_size);
   check(read_bytes == FLAGS_page_size);
}
// -------------------------------------------------------------------------------------
void BufferManager::writePage(u8 *source, u64 pid)
{
   assert(u64(source) % 512 == 0);
   s64 write_bytes = pwrite(ssd_fd, source, FLAGS_page_size, pid * FLAGS_page_size);
   check(FLAGS_page_size == write_bytes);
}
// -------------------------------------------------------------------------------------
void BufferManager::flush()
{
   fdatasync(ssd_fd);
}
}
// -------------------------------------------------------------------------------------