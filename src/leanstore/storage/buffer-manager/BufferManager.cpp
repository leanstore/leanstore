#include "BufferManager.hpp"
#include "BufferFrame.hpp"
#include "AsyncWriteBuffer.hpp"
#include "leanstore/random-generator/RandomGenerator.hpp"
#include "leanstore/storage/btree/BTreeOptimistic.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_OFF
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/rotating_file_sink.h" // support for rotating file logging
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <unistd.h>
#include <emmintrin.h>
#include <set>
// -------------------------------------------------------------------------------------
DEFINE_uint32(dram_pages, 10 * 1000, "");
DEFINE_uint32(ssd_pages, 100 * 1000, "");
DEFINE_string(ssd_path, "leanstore", "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(cooling_threshold, 10, "Start cooling pages when <= x% are free");
// -------------------------------------------------------------------------------------
DEFINE_uint32(background_write_sleep, 10, "us");
DEFINE_uint32(write_buffer_size, 512, "");
DEFINE_uint32(async_batch_size, 128, "");
// -------------------------------------------------------------------------------------
namespace leanstore {
BufferManager::BufferManager(bool truncate_ssd_file)
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      const u64 dram_total_size = sizeof(BufferFrame) * u64(FLAGS_dram_pages);
      bfs = reinterpret_cast<BufferFrame *>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
      madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      for ( u64 bf_i = 0; bf_i < FLAGS_dram_pages; bf_i++ ) {
         dram_free_bfs.push(new(bfs + bf_i) BufferFrame());
      }
      dram_free_bfs_counter = FLAGS_dram_pages;
   }
   // -------------------------------------------------------------------------------------
   // Init SSD pool
   const u32 ssd_total_size = FLAGS_ssd_pages * PAGE_SIZE;
   int flags = O_RDWR | O_DIRECT | O_CREAT;
   if ( truncate_ssd_file ) {
      flags |= O_TRUNC;
   }
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   check(ssd_fd > -1);
   check(ftruncate(ssd_fd, ssd_total_size) == 0);
   if ( fcntl(ssd_fd, F_GETFL) == -1 ) {
      throw Generic_Exception("Can not initialize SSD storage: " + FLAGS_ssd_path);
   }
   // -------------------------------------------------------------------------------------
   for ( u64 pid = 0; pid < FLAGS_ssd_pages; pid++ ) {
      cooling_io_ht.emplace(std::piecewise_construct, std::forward_as_tuple(pid), std::forward_as_tuple());
      ssd_free_pages.push(pid);
   }
   // -------------------------------------------------------------------------------------
   // Background threads
   std::thread page_provider_thread([&]() {
      pthread_setname_np(pthread_self(), "page_provider");
      auto logger = spdlog::rotating_logger_mt("PageProviderThread", "page_provider.txt", 1024 * 1024, 1);
      // -------------------------------------------------------------------------------------
      // Init AIO Context
      // TODO: own variable for page provider write buffer size
      AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_write_buffer_size);
      // -------------------------------------------------------------------------------------
      BufferFrame *r_buffer = &randomBufferFrame();
      u32 pick_random_again_counter = 0;
      bool to_cooling_stage = 1;
      //TODO: REWRITE!!
      while ( bg_threads_keep_running ) {
         try {
            if ( to_cooling_stage  ) {
               // unswizzle pages (put in the cooling stage)
               if ( dram_free_bfs_counter * 100.0 / FLAGS_dram_pages <= FLAGS_cooling_threshold ) {
                  ReadGuard r_guard(r_buffer->header.lock);
                  const bool is_cooling_candidate = r_buffer->header.state == BufferFrame::State::HOT; // && !rand_buffer->header.isWB
                  if ( !is_cooling_candidate ) {
                     r_buffer = &randomBufferFrame();
                     if ( pick_random_again_counter++ == 5 ) {
                        to_cooling_stage = !to_cooling_stage;
                        pick_random_again_counter = 0;
                     }
                     continue;
                  }
                  r_guard.recheck();
                  // -------------------------------------------------------------------------------------
                  bool picked_a_child_instead = false;
                  dt_registry.iterateChildrenSwips(r_buffer->page.dt_id,
                                                   *r_buffer, r_guard, [&](Swip<BufferFrame> &swip) {
                             if ( swip.isSwizzled()) {
                                r_buffer = &swip.asBufferFrame();
                                r_guard.recheck();
                                picked_a_child_instead = true;
                                return false;
                             }
                             r_guard.recheck();
                             return true;
                          });
                  if ( picked_a_child_instead ) {
                     logger->info("picked a child instead");
                     continue; //restart the inner loop
                  }
                  // -------------------------------------------------------------------------------------
                  {
                     ExclusiveGuard r_x_guad(r_guard);
                     ParentSwipHandler parent_handler = dt_registry.findParent(r_buffer->page.dt_id, *r_buffer, r_guard);
                     ExclusiveGuard p_x_guard(parent_handler.guard);
                     std::lock_guard g_guard(global_mutex); // must accquire the mutex before exclusive locks
                     rassert(parent_handler.guard.local_version == parent_handler.guard.version_ptr->load());
                     rassert(parent_handler.swip.bf == r_buffer);
                     logger->info("cooling PID = {}", r_buffer->header.pid);
                     parent_handler.swip.unswizzle(r_buffer->header.pid);
                     CIOFrame &cio_frame = cooling_io_ht[r_buffer->header.pid];
                     cio_frame.state = CIOFrame::State::COOLING;
                     cooling_fifo_queue.push_back(r_buffer);
                     cio_frame.fifo_itr = --cooling_fifo_queue.end();
                     r_buffer->header.state = BufferFrame::State::COLD;
                     // -------------------------------------------------------------------------------------
                     stats.unswizzled_pages_counter++;
                  }
                  r_buffer = &randomBufferFrame();
                  to_cooling_stage = !to_cooling_stage;
               }
            } else { // out of the cooling stage
               // AsyncWrite (for dirty) or remove (clean) the oldest (n) pages from fifo
               std::unique_lock g_guard(global_mutex);
               //TODO: other variable than async_batch_size
               auto bf_itr = cooling_fifo_queue.begin();
               while ( bf_itr != cooling_fifo_queue.end()) {
                  BufferFrame &bf = **bf_itr;
                  auto next_bf_tr = std::next(bf_itr, 1);
                  PID pid = bf.header.pid;
                  // TODO: can we write multiple  versions sim ?
                  // TODO: current implementation assume that checkpoint thread does not touch the
                  // the cooled pages
                  if ( bf.header.isWB == false ) {
                     if ( !bf.isDirty()) {
                        std::lock_guard reservoir_guard(reservoir_mutex);
                        // Reclaim buffer frame
                        CIOFrame &cio_frame = cooling_io_ht[pid];
                        rassert(cio_frame.state == CIOFrame::State::COOLING);
                        cooling_fifo_queue.erase(bf_itr);
                        cio_frame.state = CIOFrame::State::NOT_LOADED;
                        // -------------------------------------------------------------------------------------
                        bf.header.state = BufferFrame::State::FREE;
                        bf.header.pid = 0;
                        bf.header.isWB = false;
                        // -------------------------------------------------------------------------------------
                        dram_free_bfs.push(&bf);
                        dram_free_bfs_counter++;
                     } else {
                        //TODO: optimize this path: an array for shared/ex guards and writeasync out of the global lock
                        if ( async_write_buffer.add(bf)) {
                           bf.header.isWB = true;
                        }
                     }
                  }
                  bf_itr = next_bf_tr;
               }
               g_guard.unlock();
               async_write_buffer.submitIfNecessary([&](BufferFrame &written_bf, u64 written_lsn) {
                  while ( true ) {
                     try {
                        ReadGuard guard(written_bf.header.lock);
                        ExclusiveGuard x_guard(guard);
                        rassert(written_bf.header.isWB == true);
                        written_bf.header.lastWrittenLSN = written_lsn;
                        written_bf.header.isWB = false;
                        // -------------------------------------------------------------------------------------
                        stats.flushed_pages_counter++;
                        return;
                     } catch ( RestartException e ) {
                     }
                  }
               }, FLAGS_async_batch_size); // TODO: own gflag for batch size
               to_cooling_stage = !to_cooling_stage;
            }
         } catch ( RestartException e ) {
         }
      }
      bg_threads_counter--;
      logger->info("end");
   });
   bg_threads_counter++;
   page_provider_thread.detach();
   // -------------------------------------------------------------------------------------
   //
   std::thread checkpoint_thread([&]() {
      pthread_setname_np(pthread_self(), "checkpoint");
      auto logger = spdlog::rotating_logger_mt("CheckPointThread", "checkpoint_thread.txt", 1024 * 1024, 1);
      // -------------------------------------------------------------------------------------
      // Init AIO stack
      AsyncWriteBuffer async_write_buffer(PAGE_SIZE, FLAGS_write_buffer_size, ssd_fd);
      // -------------------------------------------------------------------------------------
      //TODO:
      bg_threads_counter--;
      logger->info("end");
   });
   bg_threads_counter++;
   checkpoint_thread.detach();
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::randomBufferFrame()
{
   auto rand_buffer_i = RandomGenerator::getRand<u64>(0, FLAGS_dram_pages);
   return bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame &BufferManager::allocatePage()
{
   std::lock_guard lock(reservoir_mutex);
   if ( !ssd_free_pages.size()) {
      throw Generic_Exception("Ran out of SSD Pages");
   }
   if ( !dram_free_bfs.size()) {
      throw RestartException(); //TODO: out of memory ?
   }
   auto free_pid = ssd_free_pages.front();
   ssd_free_pages.pop();
   auto free_bf = dram_free_bfs.front();
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   free_bf->header.pid = free_pid;
   free_bf->header.lock = 2; // Write lock
   free_bf->header.state = BufferFrame::State::HOT;
   free_bf->header.lastWrittenLSN = free_bf->page.LSN = 0;
   // -------------------------------------------------------------------------------------
   dram_free_bfs.pop();
   dram_free_bfs_counter--;
   // -------------------------------------------------------------------------------------
   return *free_bf;
}
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::resolveSwip(ReadGuard &swip_guard, Swip<BufferFrame> &swip_value) // throws RestartException
{
   static auto logger = spdlog::rotating_logger_mt("ResolveSwip", "resolve_swip.txt", 1024 * 1024, 1);
   if ( swip_value.isSwizzled()) {
      BufferFrame &bf = swip_value.asBufferFrame();
      swip_guard.recheck();
      return bf;
   }
   // -------------------------------------------------------------------------------------
   // Hack around taking the global lock
   {
//      u32 mask = 1;
//      u32 const max = 64; //MAX_BACKOFF
//      while ((dram_free_bfs_counter.load() == 0)) { //spin bf_s_lock
//         for ( u32 i = mask; i; --i ) {
//            _mm_pause();
//         }
//         mask = mask < max ? mask << 1 : max;
//      }
   }
   // -------------------------------------------------------------------------------------
   std::unique_lock g_guard(global_mutex);
   const PID pid = swip_value.asPageID();
   logger->info("WorkerThread: checking the CIOTable for pid {}", pid);
   swip_guard.recheck();
   // -------------------------------------------------------------------------------------
   CIOFrame &cio_frame = cooling_io_ht[pid];
   if ( cio_frame.state == CIOFrame::State::NOT_LOADED ) {
      logger->info("WorkerThread::resolveSwip:not loaded state");
      // First check if we have enough pages
      std::unique_lock reservoir_guard(reservoir_mutex);
      if ( !dram_free_bfs.size()) {
         throw RestartException();
      }
      BufferFrame &bf = *dram_free_bfs.front();
      dram_free_bfs.pop();
      dram_free_bfs_counter--;
      reservoir_guard.unlock();
      // -------------------------------------------------------------------------------------
      cio_frame.readers_counter++;
      cio_frame.state = CIOFrame::State::READING;
      cio_frame.mutex.lock();
      // -------------------------------------------------------------------------------------
      g_guard.unlock();
      // -------------------------------------------------------------------------------------
      readPageSync(pid, bf.page);
      rassert(bf.page.magic_debugging_number == pid);
      // ATTENTION: Fill the BF
      bf.header.lastWrittenLSN = bf.page.LSN;
      bf.header.state = BufferFrame::State::COLD;
      bf.header.isWB = false;
      bf.header.pid = pid;
      // -------------------------------------------------------------------------------------
      // Move to cooling stage
      g_guard.lock();
      cio_frame.state = CIOFrame::State::COOLING;
      cooling_fifo_queue.push_back(&bf);
      cio_frame.fifo_itr = --cooling_fifo_queue.end();
      rassert(*cio_frame.fifo_itr == &bf);
      g_guard.unlock();
      cio_frame.mutex.unlock();
      throw RestartException();
      // TODO: do we really need to clean up ?
   }
   if ( cio_frame.state == CIOFrame::State::READING ) {
      logger->info("WorkerThread::resolveSwip:Reading state");
      cio_frame.readers_counter++;
      g_guard.unlock();
      cio_frame.mutex.lock();
      cio_frame.readers_counter--;
      cio_frame.mutex.unlock();
      throw RestartException();
   }
   /*
    * Lessons learned here:
    * don't catch a restart exception here
    * Whenever we fail to accquire a lock or witness a version change
    * then we have to read the value ! (update SharedGuard)
    * otherwise we would stick with the wrong version the whole time
    * and nasty things would happen
    */
   if ( cio_frame.state == CIOFrame::State::COOLING ) {
      logger->info("WorkerThread::resolveSwip:Cooling state");
      ExclusiveGuard x_lock(swip_guard);
      BufferFrame *bf = *cio_frame.fifo_itr;
      cooling_fifo_queue.erase(cio_frame.fifo_itr);
      rassert(bf->header.state == BufferFrame::State::COLD);
      cio_frame.state = CIOFrame::State::NOT_LOADED;
      bf->header.state = BufferFrame::State::HOT;
      // -------------------------------------------------------------------------------------
      swip_value.swizzle(bf);
      // -------------------------------------------------------------------------------------
      stats.swizzled_pages_counter++;
      logger->info("WorkerThread::resolveSwip:Cooling state - swizzled in");
      return *bf;
   }
   // it is a bug signal, if the page was hot then we should never hit this path
   UNREACHABLE();
}
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8 *destination)
{
   rassert(u64(destination) % 512 == 0);
   s64 read_bytes = pread(ssd_fd, destination, PAGE_SIZE, pid * PAGE_SIZE);
   if ( read_bytes != PAGE_SIZE ) {
      cerr << pid << endl;
   }
   rassert(read_bytes == PAGE_SIZE);
}
// -------------------------------------------------------------------------------------
void BufferManager::fDataSync()
{
   fdatasync(ssd_fd);
}
// -------------------------------------------------------------------------------------
// Datastructures management
// -------------------------------------------------------------------------------------
void BufferManager::registerDatastructureType(leanstore::DTType type, leanstore::DTRegistry::DTMeta dt_meta)
{
   dt_registry.dt_types_ht[type] = dt_meta;
}
// -------------------------------------------------------------------------------------
DTID BufferManager::registerDatastructureInstance(leanstore::DTType type, void *root_object)
{
   DTID new_instance_id = dt_registry.dt_types_ht[type].instances_counter++;
   dt_registry.dt_instances_ht.insert({new_instance_id, {type, root_object}});
   return new_instance_id;
}
// -------------------------------------------------------------------------------------
void BufferManager::flushDropAllPages()
{
   std::set<u64> dirty_pages;
   FLAGS_cooling_threshold = 100;
   auto free_bfs = dram_free_bfs_counter.load();
   while ( free_bfs != FLAGS_dram_pages ) {
      cout << FLAGS_dram_pages - free_bfs << " left" << endl;
      sleep(1);
      free_bfs = dram_free_bfs_counter.load();
   }
   fDataSync();
   cout << FLAGS_dram_pages - free_bfs << " left" << endl;
}
// -------------------------------------------------------------------------------------
unique_ptr<BufferManager> BMC::global_bf(nullptr);
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
   bg_threads_keep_running = false;
   while ( bg_threads_counter ) {
      _mm_pause();
   }
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   stopBackgroundThreads();
   const u64 dram_total_size = sizeof(BufferFrame) * u64(FLAGS_dram_pages);
   close(ssd_fd);
   ssd_fd = -1;
   munmap(bfs, dram_total_size);
   // -------------------------------------------------------------------------------------
   cout << "Stats" << endl;
   cout << "swizzled counter = " << stats.swizzled_pages_counter << endl;
   cout << "unswizzled counter = " << stats.unswizzled_pages_counter << endl;
   cout << "flushed counter = " << stats.flushed_pages_counter << endl;
   // -------------------------------------------------------------------------------------
   // TODO: save states in YAML
   // -------------------------------------------------------------------------------------
   spdlog::drop_all();
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------