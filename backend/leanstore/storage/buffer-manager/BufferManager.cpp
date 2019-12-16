#include "BufferManager.hpp"
#include "BufferFrame.hpp"
#include "AsyncWriteBuffer.hpp"
#include "Exceptions.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/storage/btree/fs/BTreeOptimistic.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <unistd.h>
#include <emmintrin.h>
#include <set>
#include <iomanip>
#include <fstream>
#include <sys/time.h>
#include <sys/resource.h>
// -------------------------------------------------------------------------------------
// Local GFlags
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
BufferManager::BufferManager()
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      dram_pool_size = FLAGS_dram_gib * 1024 * 1024 * 1024 / sizeof(BufferFrame);
      const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
      bfs = reinterpret_cast<BufferFrame *>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
      madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      madvise(bfs, dram_total_size, MADV_DONTFORK); // O_DIRECT does not work with forking.
      // -------------------------------------------------------------------------------------
      for ( u64 bf_i = 0; bf_i < dram_pool_size; bf_i++ ) {
         dram_free_list.push(*new(bfs + bf_i) BufferFrame());
      }
      // -------------------------------------------------------------------------------------
   }
   // -------------------------------------------------------------------------------------
   // Init SSD pool
   int flags = O_RDWR | O_DIRECT;
   if ( FLAGS_trunc ) {
      flags |= O_TRUNC | O_CREAT;
   }
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   posix_check(ssd_fd > -1);
   if ( FLAGS_falloc > 0 ) {
      const u64 gib_size = 1024ull * 1024ull * 1024ull;
      auto dummy_data = (u8 *) aligned_alloc(512, gib_size);
      for ( u64 i = 0; i < FLAGS_falloc; i++ ) {
         const int ret = pwrite(ssd_fd, dummy_data, gib_size, gib_size * i);
         posix_check(ret == gib_size);
      }
      free(dummy_data);
      fsync(ssd_fd);
   }
   ensure (fcntl(ssd_fd, F_GETFL) != -1);
   // -------------------------------------------------------------------------------------
   // Initialize partitions
   partitions_count = (1 << FLAGS_partition_bits);
   partitions_mask = partitions_count - 1;
   const u64 cooling_bfs_upper_bound = FLAGS_cool * 1.5 * dram_pool_size / 100.0 / static_cast<double>(partitions_count);
   partitions = reinterpret_cast<PartitionTable *>(malloc(sizeof(PartitionTable) * partitions_count));
   for ( u64 p_i = 0; p_i < partitions_count; p_i++ ) {
      new(partitions + p_i) PartitionTable(utils::getBitsNeeded(cooling_bfs_upper_bound));
   }
   // -------------------------------------------------------------------------------------
   // Background threads
   // -------------------------------------------------------------------------------------
   std::thread page_provider_thread([&]() {
      // https://linux.die.net/man/2/setpriority
      if ( FLAGS_root ) {
         posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
      }
      pageProviderThread();
   });
   {
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(0, &cpuset);
      posix_check(pthread_setaffinity_np(page_provider_thread.native_handle(),
                                         sizeof(cpu_set_t), &cpuset) == 0);
   }
   page_provider_thread.detach();
   // -------------------------------------------------------------------------------------
   if ( FLAGS_file_suffix == "" ) {
      file_suffix = to_string(chrono::high_resolution_clock::now().time_since_epoch().count());
   } else {
      file_suffix = FLAGS_file_suffix;
   }
   std::thread phase_timer_thread([&]() { debuggingThread(); });
   bg_threads_counter++;
   phase_timer_thread.detach();
}
// -------------------------------------------------------------------------------------
void BufferManager::pageProviderThread()
{
   pthread_setname_np(pthread_self(), "page_provider");
   // -------------------------------------------------------------------------------------
   // Init AIO Context
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_async_batch_size);
   // -------------------------------------------------------------------------------------
   BufferFrame *r_buffer = &randomBufferFrame();
   const u64 free_pages_limit = FLAGS_free * dram_pool_size / 100.0;
   const u64 cooling_pages_limit = FLAGS_cool * dram_pool_size / 100.0;
   // -------------------------------------------------------------------------------------
   auto phase_1_condition = [&]() {
      return (dram_free_list.counter + cooling_bfs_counter) < cooling_pages_limit;
   };
   auto phase_2_3_condition = [&]() {
      return (dram_free_list.counter < free_pages_limit);
   };
   // -------------------------------------------------------------------------------------
   while ( bg_threads_keep_running ) {
      /*
       * Phase 1:
       */
      // phase_1:
      auto phase_1_begin = chrono::high_resolution_clock::now();
      {
         try {
            while ( phase_1_condition()) {
               debugging_counters.phase_1_counter++;
               // -------------------------------------------------------------------------------------
               // unswizzle pages (put in the cooling stage)
               ReadGuard r_guard(r_buffer->header.lock);
               const bool is_cooling_candidate = !(r_buffer->header.lock & WRITE_LOCK_BIT) && r_buffer->header.state == BufferFrame::State::HOT; // && !rand_buffer->header.isWB
               if ( !is_cooling_candidate ) {
                  r_buffer = &randomBufferFrame();
                  continue;
               }
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               bool picked_a_child_instead = false;
               dt_registry.iterateChildrenSwips(r_buffer->page.dt_id,
                                                *r_buffer, [&](Swip<BufferFrame> &swip) {
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
                  continue; //restart the inner loop
               }
               // -------------------------------------------------------------------------------------
               // Suitable page founds, lets unswizzle
               {
                  const PID pid = r_buffer->header.pid;
                  ParentSwipHandler parent_handler = dt_registry.findParent(r_buffer->page.dt_id, *r_buffer);
                  ExclusiveGuard p_x_guard(parent_handler.guard);
                  ExclusiveGuard r_x_guad(r_guard);
                  PartitionTable &partition = getPartition(pid);
                  std::lock_guard g_guard(partition.cio_mutex);
                  // -------------------------------------------------------------------------------------
                  assert(r_buffer->header.state == BufferFrame::State::HOT);
                  assert(parent_handler.guard.local_version == parent_handler.guard.version_ptr->load());
                  assert(parent_handler.swip.bf == r_buffer);
                  // -------------------------------------------------------------------------------------
                  if ( partition.ht.has(r_buffer->header.pid)) {
                     // This means that some thread is still in reading stage (holding cio_mutex)
                     r_buffer = &randomBufferFrame();
                     continue;
                  }
                  CIOFrame &cio_frame = partition.ht.insert(pid);
                  assert ((partition.ht.has(r_buffer->header.pid)));
                  cio_frame.state = CIOFrame::State::COOLING;
                  partition.cooling_queue.push_back(r_buffer);
                  cio_frame.fifo_itr = --partition.cooling_queue.end();
                  r_buffer->header.state = BufferFrame::State::COLD;
                  r_buffer->header.isCooledBecauseOfReading = false;
                  parent_handler.swip.unswizzle(r_buffer->header.pid);
                  cooling_bfs_counter++;
                  // -------------------------------------------------------------------------------------
                  debugging_counters.unswizzled_pages_counter++;
                  // -------------------------------------------------------------------------------------
                  if ( !phase_1_condition()) {
                     r_buffer = &randomBufferFrame();
                     break;
                  }
               }
               r_buffer = &randomBufferFrame();
               // -------------------------------------------------------------------------------------
            }
         } catch ( RestartException e ) {
            r_buffer = &randomBufferFrame();
         }
      };
      auto phase_1_end = chrono::high_resolution_clock::now();
      debugging_counters.phase_1_ms += (chrono::duration_cast<chrono::microseconds>(phase_1_end - phase_1_begin).count());
      // -------------------------------------------------------------------------------------
      const u64 pages_to_iterate_globally = (dram_free_list.counter < free_pages_limit) ? free_pages_limit - dram_free_list.counter : 0;
      const u64 pages_to_iterate_partition = pages_to_iterate_globally / partitions_count;
      // phase_2_3:
      if ( phase_2_3_condition())
         for ( u64 p_i = 0; p_i < partitions_count; p_i++ ) {
            PartitionTable &partition = partitions[p_i];
            /*
             * Phase 2:
             * Iterate over all partitions, in each partition:
             * iterate over the end of FIFO queue,
             */
            // phase_2:
            auto phase_2_begin = chrono::high_resolution_clock::now();
            {
               u64 pages_left_to_iterate_partition = pages_to_iterate_partition;
               std::unique_lock g_guard(partition.cio_mutex);
               auto bf_itr = partition.cooling_queue.begin();
               while ( pages_left_to_iterate_partition-- && bf_itr != partition.cooling_queue.end()) {
                  BufferFrame &bf = **bf_itr;
                  auto next_bf_tr = std::next(bf_itr, 1);
                  const PID pid = bf.header.pid;
                  // -------------------------------------------------------------------------------------
                  if ( !bf.header.isCooledBecauseOfReading ) {
                     assert(!bf.header.isWB);
                     if ( bf.isDirty()) {
                        if ( !async_write_buffer.add(bf)) {
                           // AsyncBuffer is full, break and start with phase 3
                           break;
                        }
                     } else {
                        try {
                           ExclusiveGuardTry w_x_guard(bf.header.lock);
                           // Reclaim buffer frame
                           HashTable::Handler frame_handler = partition.ht.lookup(pid);
                           assert(frame_handler);
                           assert(frame_handler.frame().state == CIOFrame::State::COOLING);
                           assert(bf.header.state == BufferFrame::State::COLD);
                           // -------------------------------------------------------------------------------------
                           partition.cooling_queue.erase(bf_itr);
                           partition.ht.remove(frame_handler);
                           assert(!partition.ht.has(pid));
                           // -------------------------------------------------------------------------------------
                           reclaimBufferFrame(bf);
                           // -------------------------------------------------------------------------------------
                           cooling_bfs_counter--;
                           debugging_counters.evicted_pages++;
                        } catch ( RestartException e ) {
                           assert(false);
                        }
                     }
                  }
                  bf_itr = next_bf_tr;
               }
            };
            auto phase_2_end = chrono::high_resolution_clock::now();
            /*
             * Phase 3:
             */
            // phase_3:
            auto phase_3_begin = chrono::high_resolution_clock::now();
            {
               auto submit_begin = chrono::high_resolution_clock::now();
               async_write_buffer.submit();
               auto submit_end = chrono::high_resolution_clock::now();
               // -------------------------------------------------------------------------------------
               auto poll_begin = chrono::high_resolution_clock::now();
               const u32 polled_events = async_write_buffer.pollEventsSync();
               auto poll_end = chrono::high_resolution_clock::now();
               // -------------------------------------------------------------------------------------
               auto async_wb_begin = chrono::high_resolution_clock::now();
               async_write_buffer.getWrittenBfs([&](BufferFrame &written_bf, u64 written_lsn) {
                  assert(written_bf.header.lastWrittenLSN.load() < written_lsn);
                  // -------------------------------------------------------------------------------------
                  written_bf.header.lastWrittenLSN.store(written_lsn);
                  written_bf.header.isWB.store(false);
                  debugging_counters.flushed_pages_counter++;
               }, polled_events);
               auto async_wb_end = chrono::high_resolution_clock::now();
               // -------------------------------------------------------------------------------------
               u64 pages_left_to_iterate_partition = polled_events;
               std::unique_lock g_guard(partition.cio_mutex);
               // -------------------------------------------------------------------------------------
               auto bf_itr = partition.cooling_queue.begin();
               while ( pages_left_to_iterate_partition-- && bf_itr != partition.cooling_queue.end()) {
                  BufferFrame &bf = **bf_itr;
                  auto next_bf_tr = std::next(bf_itr, 1);
                  const PID pid = bf.header.pid;
                  // -------------------------------------------------------------------------------------
                  if ( !bf.isDirty() && !bf.header.isCooledBecauseOfReading ) {
                     try {
                        ExclusiveGuardTry w_x_guard(bf.header.lock);
                        // Reclaim buffer frame
                        HashTable::Handler frame_handler = partition.ht.lookup(pid);
                        assert(frame_handler);
                        assert(frame_handler.frame().state == CIOFrame::State::COOLING);
                        assert(bf.header.state == BufferFrame::State::COLD);
                        // -------------------------------------------------------------------------------------
                        partition.cooling_queue.erase(bf_itr);
                        partition.ht.remove(frame_handler);
                        assert(!partition.ht.has(pid));
                        // -------------------------------------------------------------------------------------
                        reclaimBufferFrame(bf);
                        // -------------------------------------------------------------------------------------
                        cooling_bfs_counter--;
                        debugging_counters.evicted_pages++;
                     } catch ( RestartException e ) {
                        ensure(false);
                     }
                  }
                  // -------------------------------------------------------------------------------------
                  bf_itr = next_bf_tr;
               }
               // -------------------------------------------------------------------------------------
               debugging_counters.poll_ms += (chrono::duration_cast<chrono::microseconds>(poll_end - poll_begin).count());
               debugging_counters.async_wb_ms += (chrono::duration_cast<chrono::microseconds>(async_wb_end - async_wb_begin).count());
               debugging_counters.submit_ms += (chrono::duration_cast<chrono::microseconds>(submit_end - submit_begin).count());
            };
            auto phase_3_end = chrono::high_resolution_clock::now();
            // -------------------------------------------------------------------------------------
            debugging_counters.phase_2_ms += (chrono::duration_cast<chrono::microseconds>(phase_2_end - phase_2_begin).count());
            debugging_counters.phase_3_ms += (chrono::duration_cast<chrono::microseconds>(phase_3_end - phase_3_begin).count());
         }
      debugging_counters.pp_thread_rounds++;
   }
   bg_threads_counter--;
}
// -------------------------------------------------------------------------------------
void BufferManager::debuggingThread()
{
   pthread_setname_np(pthread_self(), "debugging_thread");
   PerfEventBlock b(e, 1);
   // -------------------------------------------------------------------------------------
   auto file_name = [&](const string prefix) {
      return prefix + "_" + file_suffix + ".csv";
   };
   // -------------------------------------------------------------------------------------
   std::ofstream pp_csv;
   string pp_csv_file_path;
   pp_csv.open(file_name("pp"), ios::out | ios::trunc);
   pp_csv << std::setprecision(2);
   // -------------------------------------------------------------------------------------
   std::ofstream workers_csv;
   string workers_csv_file_path;
   workers_csv.open(file_name("workers"), ios::out | ios::trunc);
   workers_csv << std::setprecision(2);
   // -------------------------------------------------------------------------------------
   using statCallback = std::function<void(ostream &)>;
   struct StatEntry {
      string name;
      statCallback callback;
      StatEntry(string &&n, statCallback b)
              : name(std::move(n))
                , callback(b) {}
   };
   // -------------------------------------------------------------------------------------
   vector<StatEntry> pp_stats;
   s64 local_phase_1_ms = 0, local_phase_2_ms = 0, local_phase_3_ms = 0, local_poll_ms = 0, total;
   // -------------------------------------------------------------------------------------
   pp_stats.emplace_back("p1_pct", [&](ostream &out) {
      out << (local_phase_1_ms * 100.0 / total);
   });
   pp_stats.emplace_back("p2_pct", [&](ostream &out) { out << (local_phase_2_ms * 100.0 / total); });
   pp_stats.emplace_back("p3_pct", [&](ostream &out) { out << (local_phase_3_ms * 100.0 / total); });
   pp_stats.emplace_back("poll_pct", [&](ostream &out) { out << (local_poll_ms * 100.0 / total); });
   pp_stats.emplace_back("pc1", [&](ostream &out) { out << debugging_counters.phase_1_counter.exchange(0); });
   pp_stats.emplace_back("pc2", [&](ostream &out) { out << debugging_counters.phase_2_counter.exchange(0); });
   pp_stats.emplace_back("pc3", [&](ostream &out) { out << debugging_counters.phase_3_counter.exchange(0); });
   pp_stats.emplace_back("free_pct", [&](ostream &out) { out << (dram_free_list.counter.load() * 100.0 / dram_pool_size); });
   pp_stats.emplace_back("cool_pct", [&](ostream &out) { out << (cooling_bfs_counter.load() * 100.0 / dram_pool_size); });
   pp_stats.emplace_back("evicted", [&](ostream &out) { out << (debugging_counters.evicted_pages.exchange(0) * 100.0 / dram_pool_size); });
   pp_stats.emplace_back("rounds", [&](ostream &out) { out << (debugging_counters.pp_thread_rounds.exchange(0)); });
   pp_stats.emplace_back("unswizzled", [&](ostream &out) { out << debugging_counters.unswizzled_pages_counter.exchange(0); });
   pp_stats.emplace_back("cold_hit", [&](ostream &out) { out << debugging_counters.cold_hit_counter.exchange(0); });
   pp_stats.emplace_back("cpus", [&](ostream &out) { out << b.e.getCPUs(); });
   pp_stats.emplace_back("submit_ms", [&](ostream &out) { out << debugging_counters.submit_ms.exchange(0); });
   pp_stats.emplace_back("async_mb_ws", [&](ostream &out) { out << debugging_counters.async_wb_ms.exchange(0); });
   pp_stats.emplace_back("allocate_ops", [&](ostream &out) { out << debugging_counters.allocate_operations_counter.exchange(0); });
   pp_stats.emplace_back("r_mib", [&](ostream &out) { out << (debugging_counters.read_operations_counter.exchange(0) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0); });
   pp_stats.emplace_back("w_mib", [&](ostream &out) { out << debugging_counters.awrites_submitted.exchange(0) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0; });
   // -------------------------------------------------------------------------------------
   // Print header
   pp_csv << "t";
   for ( const auto &stat :pp_stats ) {
      pp_csv << "," << stat.name;
   }
   // -------------------------------------------------------------------------------------
   workers_csv << "t,name,miss" << endl;
   // -------------------------------------------------------------------------------------
   u64 time = 0;
   // -------------------------------------------------------------------------------------
   while ( FLAGS_print_debug && bg_threads_keep_running ) {
      // -------------------------------------------------------------------------------------
      local_phase_1_ms = debugging_counters.phase_1_ms.exchange(0);
      local_phase_2_ms = debugging_counters.phase_2_ms.exchange(0);
      local_phase_3_ms = debugging_counters.phase_3_ms.exchange(0);
      local_poll_ms = debugging_counters.poll_ms.exchange(0);
      // -------------------------------------------------------------------------------------
      total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
      // -------------------------------------------------------------------------------------
      b.e.stopCounters();
      if ( total > 0 ) {
         pp_csv << time;
         for ( const auto &stat :pp_stats ) {
            pp_csv << ",";
            stat.callback(pp_csv);
         }
         pp_csv << endl;
         // -------------------------------------------------------------------------------------
         for ( const auto &dt : dt_registry.dt_instances_ht ) {
            const u64 dt_id = dt.first;
            const string &dt_name = std::get<2>(dt.second);
            workers_csv << time << "," << dt_name << "," << debugging_counters.dt_misses_counter[dt_id].exchange(0) << endl;
         }
      }
      // -------------------------------------------------------------------------------------
      b.e.startCounters();
      // -------------------------------------------------------------------------------------
      sleep(FLAGS_print_debug_interval_s);
      time += FLAGS_print_debug_interval_s;
   }
   pp_csv.close();
   bg_threads_counter--;
}
// -------------------------------------------------------------------------------------
void BufferManager::clearSSD()
{
   //TODO
}
// -------------------------------------------------------------------------------------
void BufferManager::persist()
{
   // TODO
   stopBackgroundThreads();
   flushDropAllPages();
}
// -------------------------------------------------------------------------------------
void BufferManager::restore()
{
   //TODO
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   return ssd_used_pages_counter - ssd_freed_pages_counter;
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, dram_pool_size);
   return bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame &BufferManager::allocatePage()
{
   if ( dram_free_list.counter < 10 ) {
      throw RestartException();
   }
   PID free_pid = ssd_used_pages_counter++;
   BufferFrame &free_bf = dram_free_list.pop();
   assert(free_bf.header.state == BufferFrame::State::FREE);
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   assert((free_bf.header.lock & WRITE_LOCK_BIT) == 0);
   free_bf.header.lock += WRITE_LOCK_BIT; // Write lock
   free_bf.header.pid = free_pid;
   free_bf.header.state = BufferFrame::State::HOT;
   free_bf.header.lastWrittenLSN = free_bf.page.LSN = 0;
   // -------------------------------------------------------------------------------------
   if ( ssd_used_pages_counter == dram_pool_size ) {
      cout << "------------------------------------------------------------------------------------" << endl;
      cout << "Going out of memory !" << endl;
      cout << "------------------------------------------------------------------------------------" << endl;
   }
   assert((free_bf.header.lock & WRITE_LOCK_BIT) == WRITE_LOCK_BIT);
   // -------------------------------------------------------------------------------------
   debugging_counters.allocate_operations_counter++;
   DebuggingCounters::thread_local_counters.allocate_operations_counter++;
   // -------------------------------------------------------------------------------------
   return free_bf;
}
// -------------------------------------------------------------------------------------
// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
void BufferManager::reclaimBufferFrame(BufferFrame &bf)
{
   if ( bf.header.isWB ) {
      // DO NOTHING ! we have a garbage collector ;-)
      bf.header.lock.fetch_add(WRITE_LOCK_BIT);
      cout << "garbage collector, yeah" << endl;
   } else {
      bf.reset();
      bf.header.lock.fetch_add(WRITE_LOCK_BIT);
      dram_free_list.push(bf);
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::reclaimPage(BufferFrame &bf)
{
   // TODO: reclaim bf pid
   ssd_freed_pages_counter++;
   // -------------------------------------------------------------------------------------
   reclaimBufferFrame(bf);
}
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::resolveSwip(ReadGuard &swip_guard, Swip<BufferFrame> &swip_value) // throws RestartException
{
   if ( swip_value.isSwizzled()) {
      BufferFrame &bf = swip_value.asBufferFrame();
      swip_guard.recheck();
      // -------------------------------------------------------------------------------------
      // TODO: if
//      debugging_counters.hot_hit_counter++;
//      DebuggingCounters::thread_local_counters.hot_hit_counter++;
      // -------------------------------------------------------------------------------------
      return bf;
   }
   // -------------------------------------------------------------------------------------
   const PID pid = swip_value.asPageID();
   PartitionTable &partition = getPartition(pid);
   std::unique_lock g_guard(partition.cio_mutex);
   swip_guard.recheck();
   assert(!swip_value.isSwizzled());
   // -------------------------------------------------------------------------------------
   auto frame_handler = partition.ht.lookup(pid);
   if ( !frame_handler ) {
      if ( dram_free_list.counter < 10 ) {
         g_guard.unlock();
         throw RestartException();
      }
      BufferFrame &bf = dram_free_list.pop();
      CIOFrame &cio_frame = partition.ht.insert(pid);
      assert(bf.header.state == BufferFrame::State::FREE);
      assert((bf.header.lock & WRITE_LOCK_BIT) == 0);
      // -------------------------------------------------------------------------------------
      cio_frame.state = CIOFrame::State::READING;
      cio_frame.readers_counter = 1;
      cio_frame.mutex.lock();
      // -------------------------------------------------------------------------------------
      g_guard.unlock();
      // -------------------------------------------------------------------------------------
      readPageSync(pid, bf.page);
      debugging_counters.dt_misses_counter[bf.page.dt_id]++;
      assert(bf.page.magic_debugging_number == pid);
      // -------------------------------------------------------------------------------------
      // ATTENTION: Fill the BF
      assert(!bf.header.isWB);
      bf.header.lastWrittenLSN = bf.page.LSN;
      bf.header.state = BufferFrame::State::COLD;
      bf.header.pid = pid;
      // -------------------------------------------------------------------------------------
      // Move to cooling stage
      g_guard.lock();
      cio_frame.state = CIOFrame::State::COOLING;
      partition.cooling_queue.push_back(&bf);
      cio_frame.fifo_itr = --partition.cooling_queue.end();
      cooling_bfs_counter++;
      // -------------------------------------------------------------------------------------
      bf.header.isCooledBecauseOfReading = true;
      // -------------------------------------------------------------------------------------
      g_guard.unlock();
      cio_frame.mutex.unlock();
      // -------------------------------------------------------------------------------------
      throw RestartException();
   }
   // -------------------------------------------------------------------------------------
   CIOFrame &cio_frame = frame_handler.frame();
   // -------------------------------------------------------------------------------------
   if ( cio_frame.state == CIOFrame::State::READING ) {
      cio_frame.readers_counter++;
      g_guard.unlock();
      cio_frame.mutex.lock();
      cio_frame.mutex.unlock();
      // -------------------------------------------------------------------------------------
      assert(partition.ht.has(pid));
      if ( cio_frame.readers_counter.fetch_add(-1) == 1 ) {
         g_guard.lock();
         if ( cio_frame.readers_counter == 0 ) {
            partition.ht.remove(pid);
         }
         g_guard.unlock();
      }
      // -------------------------------------------------------------------------------------
      throw RestartException();
   }
   // -------------------------------------------------------------------------------------
   if ( cio_frame.state == CIOFrame::State::COOLING ) {
      // -------------------------------------------------------------------------------------
      // We have to exclusively lock the bf because the page provider thread will try to evict them when its IO is done
      BufferFrame *bf = *cio_frame.fifo_itr;
      ReadGuard bf_guard(bf->header.lock);
      ExclusiveGuard swip_x_guard(swip_guard);
      ExclusiveGuard bf_x_guard(bf_guard);
      // -------------------------------------------------------------------------------------
      assert(bf->header.pid == pid);
      swip_value.swizzle(bf);
      partition.cooling_queue.erase(cio_frame.fifo_itr);
      cooling_bfs_counter--;
      assert(bf->header.state == BufferFrame::State::COLD);
      bf->header.state = BufferFrame::State::HOT; // ATTENTION: SET TO HOT AFTER IT IS SWIZZLED IN
      // -------------------------------------------------------------------------------------
      // Simply written, let the compiler optimize it
      bool should_clean = true;
      if ( bf->header.isCooledBecauseOfReading ) {
         if ( cio_frame.readers_counter.fetch_add(-1) > 1 ) {
            should_clean = false;
         }
      } else {
         debugging_counters.cold_hit_counter++;
         DebuggingCounters::thread_local_counters.cold_hit_counter++;
      }
      if ( should_clean ) {
         partition.ht.remove(pid);
      }
      // -------------------------------------------------------------------------------------
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
   assert(u64(destination) % 512 == 0);
   s64 bytes_left = PAGE_SIZE;
   do {
      const int bytes_read = pread(ssd_fd, destination, bytes_left, pid * PAGE_SIZE + (PAGE_SIZE - bytes_left));
      assert(bytes_left > 0);
      bytes_left -= bytes_read;
   } while ( bytes_left > 0 );
   // -------------------------------------------------------------------------------------
   debugging_counters.read_operations_counter++;
   DebuggingCounters::thread_local_counters.read_operations_counter++;
}
// -------------------------------------------------------------------------------------
void BufferManager::fDataSync()
{
   fdatasync(ssd_fd);
}
// -------------------------------------------------------------------------------------
// Datastructures management
// -------------------------------------------------------------------------------------
void BufferManager::registerDatastructureType(DTType type, DTRegistry::DTMeta dt_meta)
{
   dt_registry.dt_types_ht[type] = dt_meta;
}
// -------------------------------------------------------------------------------------
DTID BufferManager::registerDatastructureInstance(DTType type, void *root_object, string name)
{
   DTID new_instance_id = dt_registry.dt_types_ht[type].instances_counter++;
   dt_registry.dt_instances_ht.insert({new_instance_id, {type, root_object, name}});
   // -------------------------------------------------------------------------------------
   debugging_counters.dt_misses_counter[new_instance_id] = 0;
   // -------------------------------------------------------------------------------------
   return new_instance_id;
}
// -------------------------------------------------------------------------------------
// Make sure all worker threads are off
void BufferManager::flushDropAllPages()
{
   //TODO
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
PartitionTable &BufferManager::getPartition(PID pid)
{
   const u64 partition_i = pid & partitions_mask;
   assert(partition_i < partitions_count);
   return partitions[partition_i];
}
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
   free(partitions);
   // -------------------------------------------------------------------------------------
   const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
   close(ssd_fd);
   ssd_fd = -1;
   munmap(bfs, dram_total_size);
}
// -------------------------------------------------------------------------------------
BufferManager *BMC::global_bf(nullptr);
}
}
// -------------------------------------------------------------------------------------
