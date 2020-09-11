#include "BufferManager.hpp"

#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/counters/PPCounters.hpp"
#include "leanstore/counters/ThreadCounters.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <iomanip>
#include <set>
// -------------------------------------------------------------------------------------
// Local GFlags
// -------------------------------------------------------------------------------------
using std::thread;
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
BufferManager::BufferManager()
{
  // -------------------------------------------------------------------------------------
  // Init DRAM pool
  {
    dram_pool_size = FLAGS_dram_gib * 1024 * 1024 * 1024 / sizeof(BufferFrame);
    const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
    bfs = reinterpret_cast<BufferFrame*>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    madvise(bfs, dram_total_size, MADV_HUGEPAGE);
    madvise(bfs, dram_total_size,
            MADV_DONTFORK);  // O_DIRECT does not work with forking.
    // -------------------------------------------------------------------------------------
    // Initialize partitions
    partitions_count = (1 << FLAGS_partition_bits);
    partitions_mask = partitions_count - 1;
    const u64 free_bfs_limit = std::ceil((FLAGS_free_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(partitions_count));
    const u64 cooling_bfs_upper_bound = std::ceil((FLAGS_cool_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(partitions_count));
    partitions = reinterpret_cast<Partition*>(malloc(sizeof(Partition) * partitions_count));
    for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      new (partitions + p_i) Partition(free_bfs_limit, cooling_bfs_upper_bound);
    }
    // -------------------------------------------------------------------------------------
    utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      u64 p_i = 0;
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
        partitions[p_i].dram_free_list.push(*new (bfs + bf_i) BufferFrame());
        p_i = (p_i + 1) % partitions_count;
      }
    });
    // -------------------------------------------------------------------------------------
  }
  // -------------------------------------------------------------------------------------
  // Init SSD pool
  int flags = O_RDWR | O_DIRECT;
  if (FLAGS_trunc) {
    flags |= O_TRUNC | O_CREAT;
  }
  ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
  posix_check(ssd_fd > -1);
  if (FLAGS_falloc > 0) {
    const u64 gib_size = 1024ull * 1024ull * 1024ull;
    auto dummy_data = (u8*)aligned_alloc(512, gib_size);
    for (u64 i = 0; i < FLAGS_falloc; i++) {
      const int ret = pwrite(ssd_fd, dummy_data, gib_size, gib_size * i);
      posix_check(ret == gib_size);
    }
    free(dummy_data);
    fsync(ssd_fd);
  }
  ensure(fcntl(ssd_fd, F_GETFL) != -1);
  // -------------------------------------------------------------------------------------
  // Background threads
  // -------------------------------------------------------------------------------------
  // Page Provider threads
  if (FLAGS_pp_threads) {  // make it optional for pure in-memory experiments
    std::vector<thread> pp_threads;
    const u64 partitions_per_thread = partitions_count / FLAGS_pp_threads;
    ensure(FLAGS_pp_threads <= partitions_count);
    const u64 extra_partitions_for_last_thread = partitions_count % FLAGS_pp_threads;
    // -------------------------------------------------------------------------------------
    for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
      pp_threads.emplace_back(
          [&](u64 t_i, u64 p_begin, u64 p_end) {
            ThreadCounters::registerThread("pp_" + std::to_string(t_i));
            // https://linux.die.net/man/2/setpriority
            if (FLAGS_root) {
              posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
            }
            pageProviderThread(p_begin, p_end);
          },
          t_i, t_i * partitions_per_thread,
          ((t_i + 1) * partitions_per_thread) + ((t_i == FLAGS_pp_threads - 1) ? extra_partitions_for_last_thread : 0));
      bg_threads_counter++;
    }
    for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
      thread& page_provider_thread = pp_threads[t_i];
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(t_i, &cpuset);
      posix_check(pthread_setaffinity_np(page_provider_thread.native_handle(), sizeof(cpu_set_t), &cpuset) == 0);
      page_provider_thread.detach();
    }
  }
}
// -------------------------------------------------------------------------------------
void BufferManager::pageProviderThread(u64 p_begin, u64 p_end)  // [p_begin, p_end)
{
  pthread_setname_np(pthread_self(), "page_provider");
  using Time = decltype(std::chrono::high_resolution_clock::now());
  // -------------------------------------------------------------------------------------
  // Init AIO Context
  AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_async_batch_size);
  // -------------------------------------------------------------------------------------
  auto phase_1_condition = [&](Partition& p) { return (p.dram_free_list.counter + p.cooling_bfs_counter) < p.cooling_bfs_limit; };  //
  auto phase_2_3_condition = [&](Partition& p) { return (p.dram_free_list.counter < p.free_bfs_limit); };
  // -------------------------------------------------------------------------------------
  while (bg_threads_keep_running) {
    /*
     * Phase 1: unswizzle pages (put in the cooling stage)
     */
    [[maybe_unused]] Time phase_1_begin, phase_1_end;
    COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
    BufferFrame* volatile r_buffer = &randomBufferFrame();  // Attention: we may set the r_buffer to a child of a bf instead of random
    while (true) {
      jumpmuTry()
      {
        while (phase_1_condition(randomPartition())) {
          COUNTERS_BLOCK() { PPCounters::myCounters().phase_1_counter++; }
          if (r_buffer->header.latch.isExclusivelyLatched()) {
            r_buffer = &randomBufferFrame();
            continue;
          }
          OptimisticGuard r_guard(r_buffer->header.latch, FALLBACK_METHOD::JUMP);
          // -------------------------------------------------------------------------------------
          const u64 partition_i = getPartitionID(r_buffer->header.pid);
          static_cast<void>(partition_i);
          const bool is_cooling_candidate =
              (!r_buffer->header.isWB && !(r_buffer->header.latch.isExclusivelyLatched()) &&  // (partition_i) >= p_begin && (partition_i) < p_end &&
               r_buffer->header.state == BufferFrame::State::HOT);                            // && !rand_buffer->header.isWB
          if (!is_cooling_candidate) {
            r_buffer = &randomBufferFrame();
            continue;
          }
          r_guard.recheck();
          // -------------------------------------------------------------------------------------
          COUNTERS_BLOCK() { PPCounters::myCounters().touched_bfs_counter++; }
          // -------------------------------------------------------------------------------------
          bool picked_a_child_instead = false;
          [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
          COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
          dt_registry.iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
            if (swip.isSwizzled()) {
              r_buffer = &swip.asBufferFrame();
              r_guard.recheck();
              picked_a_child_instead = true;
              return false;
            }
            r_guard.recheck();
            return true;
          });
          COUNTERS_BLOCK()
          {
            iterate_children_begin = std::chrono::high_resolution_clock::now();
            PPCounters::myCounters().iterate_children_ms +=
                (std::chrono::duration_cast<std::chrono::microseconds>(iterate_children_end - iterate_children_begin).count());
          }
          if (picked_a_child_instead) {
            continue;  // restart the inner loop
          }
          // -------------------------------------------------------------------------------------
          [[maybe_unused]] Time find_parent_begin, find_parent_end;
          COUNTERS_BLOCK() { find_parent_begin = std::chrono::high_resolution_clock::now(); }
          ParentSwipHandler parent_handler = dt_registry.findParent(r_buffer->page.dt_id, *r_buffer);
          assert(parent_handler.parent_guard.latch_ptr != reinterpret_cast<HybridLatch*>(0x99));
          COUNTERS_BLOCK()
          {
            find_parent_end = std::chrono::high_resolution_clock::now();
            PPCounters::myCounters().find_parent_ms +=
                (std::chrono::duration_cast<std::chrono::microseconds>(find_parent_end - find_parent_begin).count());
          }
          // -------------------------------------------------------------------------------------
          r_guard.recheck();
          if (dt_registry.checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer, r_guard, parent_handler)) {
            r_buffer = &randomBufferFrame();
            continue;
          }
          r_guard.recheck();
          // -------------------------------------------------------------------------------------
          // Suitable page founds, lets unswizzle
          {
            const PID pid = r_buffer->header.pid;
            Partition& partition = getPartition(pid);
            // r_x_guard can only be acquired and release while the partition mutex is locked
            {
              JMUW<std::unique_lock<std::mutex>> g_guard(partition.cio_mutex);
              ExclusiveGuard p_x_guard(parent_handler.parent_guard);
              ExclusiveGuard r_x_guard(r_guard);
              // -------------------------------------------------------------------------------------
              assert(r_buffer->header.state == BufferFrame::State::HOT);
              assert(parent_handler.parent_guard.local_version == (parent_handler.parent_guard.latch_ptr->ref().load() & LATCH_VERSION_MASK));
              assert(parent_handler.swip.bf == r_buffer);
              // -------------------------------------------------------------------------------------
              if (partition.ht.has(r_buffer->header.pid)) {
                // This means that some thread is still in reading stage (holding
                // cio_mutex)
                partition.cio_mutex.unlock();
                r_buffer = &randomBufferFrame();
                continue;
              }
              assert(!(partition.ht.has(r_buffer->header.pid)));
              CIOFrame& cio_frame = partition.ht.insert(pid);
              assert((partition.ht.has(r_buffer->header.pid)));
              cio_frame.state = CIOFrame::State::COOLING;
              partition.cooling_queue.push_back(reinterpret_cast<BufferFrame*>(r_buffer));
              cio_frame.fifo_itr = --partition.cooling_queue.end();
              r_buffer->header.state = BufferFrame::State::COLD;
              r_buffer->header.isCooledBecauseOfReading = false;
              parent_handler.swip.unswizzle(r_buffer->header.pid);
              partition.cooling_bfs_counter++;
            }
            // -------------------------------------------------------------------------------------
            COUNTERS_BLOCK() { PPCounters::myCounters().unswizzled_pages_counter++; }
            // -------------------------------------------------------------------------------------
            if (!phase_1_condition(partition)) {
              r_buffer = &randomBufferFrame();
              break;
            }
          }
          r_buffer = &randomBufferFrame();
          // -------------------------------------------------------------------------------------
        }
        jumpmu_break;
      }
      jumpmuCatch() { r_buffer = &randomBufferFrame(); }
    }
    COUNTERS_BLOCK()
    {
      phase_1_end = std::chrono::high_resolution_clock::now();
      PPCounters::myCounters().phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
    }
    // -------------------------------------------------------------------------------------
    for (volatile u64 p_i = p_begin; p_i < p_end; p_i++) {
      Partition& partition = partitions[p_i];
      // -------------------------------------------------------------------------------------
      // phase_2_3:
      auto evict_bf = [&](BufferFrame& bf, std::list<BufferFrame*>::iterator& bf_itr) {
        jumpmuTry()
        {
          {
            // We manually lock the buffer frame, because reclaimBufferFrame is generic and called also by worker threads,
            // which have exclusive lock on it
            assert((bf.header.latch.version.load() & LATCH_EXCLUSIVE_BIT) == 0);
            bf.header.latch.mutex.lock();
            bf.header.latch.version.fetch_add(LATCH_EXCLUSIVE_BIT);
          }
          // Reclaim buffer frame
          const PID pid = bf.header.pid;
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
          partition.cooling_bfs_counter--;
          COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; }
        }
        jumpmuCatch() { ensure(false); }
      };
      if (phase_2_3_condition(partition)) {
        const s64 pages_to_iterate_partition = partition.free_bfs_limit - partition.dram_free_list.counter;
        // -------------------------------------------------------------------------------------
        /*
         * Phase 2:
         * Iterate over all partitions, in each partition:
         * iterate over the end of FIFO queue,
         */
        // phase_2:
        [[maybe_unused]] Time phase_2_begin, phase_2_end;
        COUNTERS_BLOCK() { phase_2_begin = std::chrono::high_resolution_clock::now(); }
        if (pages_to_iterate_partition > 0) {
          PPCounters::myCounters().phase_2_counter++;
          volatile u64 pages_left_to_iterate_partition = pages_to_iterate_partition;
          JMUW<std::unique_lock<std::mutex>> g_guard(partition.cio_mutex);
          auto bf_itr = partition.cooling_queue.begin();
          while (pages_left_to_iterate_partition-- && bf_itr != partition.cooling_queue.end()) {
            BufferFrame& bf = **bf_itr;
            auto next_bf_tr = std::next(bf_itr, 1);
            // -------------------------------------------------------------------------------------
            if (!bf.header.isCooledBecauseOfReading) {
              assert(!bf.header.isWB);
              if (bf.isDirty()) {
                if (!async_write_buffer.add(bf)) {
                  // AsyncBuffer is full, break and start with phase 3
                  break;
                }
              } else {
                evict_bf(bf, bf_itr);
              }
            }
            bf_itr = next_bf_tr;
          }
        };
        COUNTERS_BLOCK() { phase_2_end = std::chrono::high_resolution_clock::now(); }
        /*
         * Phase 3:
         */
        // phase_3:
        [[maybe_unused]] Time phase_3_begin, phase_3_end;
        COUNTERS_BLOCK() { phase_3_begin = std::chrono::high_resolution_clock::now(); }
        if (pages_to_iterate_partition > 0) {
          [[maybe_unused]] Time submit_begin, submit_end;
          COUNTERS_BLOCK()
          {
            PPCounters::myCounters().phase_3_counter++;
            submit_begin = std::chrono::high_resolution_clock::now();
          }
          async_write_buffer.submit();
          COUNTERS_BLOCK() { submit_end = std::chrono::high_resolution_clock::now(); }
          // -------------------------------------------------------------------------------------
          [[maybe_unused]] Time poll_begin, poll_end;
          COUNTERS_BLOCK() { poll_begin = std::chrono::high_resolution_clock::now(); }
          const u32 polled_events = async_write_buffer.pollEventsSync();
          COUNTERS_BLOCK() { poll_end = std::chrono::high_resolution_clock::now(); }
          // -------------------------------------------------------------------------------------
          [[maybe_unused]] Time async_wb_begin, async_wb_end;
          COUNTERS_BLOCK() { async_wb_begin = std::chrono::high_resolution_clock::now(); }
          async_write_buffer.getWrittenBfs(
              [&](BufferFrame& written_bf, u64 written_lsn) {
                assert(written_bf.header.lastWrittenLSN.load() < written_lsn);
                // -------------------------------------------------------------------------------------
                written_bf.header.lastWrittenLSN.store(written_lsn);
                written_bf.header.isWB.store(false);
                PPCounters::myCounters().flushed_pages_counter++;
              },
              polled_events);
          COUNTERS_BLOCK() { async_wb_end = std::chrono::high_resolution_clock::now(); }
          // -------------------------------------------------------------------------------------
          volatile u64 pages_left_to_iterate_partition = polled_events;
          JMUW<std::unique_lock<std::mutex>> g_guard(partition.cio_mutex);
          // -------------------------------------------------------------------------------------
          auto bf_itr = partition.cooling_queue.begin();
          while (pages_left_to_iterate_partition-- && bf_itr != partition.cooling_queue.end()) {
            BufferFrame& bf = **bf_itr;
            auto next_bf_tr = std::next(bf_itr, 1);
            // -------------------------------------------------------------------------------------
            if (!bf.isDirty() && !bf.header.isCooledBecauseOfReading) {
              // Ready to evict
              evict_bf(bf, bf_itr);
            }
            // -------------------------------------------------------------------------------------
            bf_itr = next_bf_tr;
          }
          // -------------------------------------------------------------------------------------
          COUNTERS_BLOCK()
          {
            PPCounters::myCounters().poll_ms += (std::chrono::duration_cast<std::chrono::microseconds>(poll_end - poll_begin).count());
            PPCounters::myCounters().async_wb_ms += (std::chrono::duration_cast<std::chrono::microseconds>(async_wb_end - async_wb_begin).count());
            PPCounters::myCounters().submit_ms += (std::chrono::duration_cast<std::chrono::microseconds>(submit_end - submit_begin).count());
            phase_3_end = std::chrono::high_resolution_clock::now();
          }
        };
        // -------------------------------------------------------------------------------------
        COUNTERS_BLOCK()
        {
          PPCounters::myCounters().phase_2_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_2_end - phase_2_begin).count());
          PPCounters::myCounters().phase_3_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_3_end - phase_3_begin).count());
        }
      }
    }  // end of partitions for
    COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
  }
  bg_threads_counter--;
}
// -------------------------------------------------------------------------------------

void BufferManager::clearSSD()
{
  // TODO
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
  // TODO
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
  return ssd_used_pages_counter - ssd_freed_pages_counter;
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::getContainingBufferFrame(const u8* ptr)
{
  u64 index = (ptr - reinterpret_cast<u8*>(bfs)) / (sizeof(BufferFrame));
  return bfs[index];
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
Partition& BufferManager::randomPartition()
{
  auto rand_partition_i = utils::RandomGenerator::getRand<u64>(0, partitions_count);
  return partitions[rand_partition_i];
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::randomBufferFrame()
{
  auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, dram_pool_size);
  return bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame& BufferManager::allocatePage()
{
  // Pick a pratition randomly
  Partition& partition = randomPartition();
  BufferFrame& free_bf = partition.dram_free_list.pop();
  PID free_pid = ssd_used_pages_counter++;
  assert(free_bf.header.state == BufferFrame::State::FREE);
  // -------------------------------------------------------------------------------------
  // Initialize Buffer Frame
  free_bf.header.latch.assertNotExclusivelyLatched();
  free_bf.header.latch.mutex.lock();
  free_bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT);  // Write lock
  free_bf.header.pid = free_pid;
  free_bf.header.state = BufferFrame::State::HOT;
  free_bf.header.lastWrittenLSN = free_bf.page.LSN = 0;
  // -------------------------------------------------------------------------------------
  if (free_pid == dram_pool_size) {
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "Going out of memory !" << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  free_bf.header.latch.assertExclusivelyLatched();
  // -------------------------------------------------------------------------------------
  COUNTERS_BLOCK() { WorkerCounters::myCounters().allocate_operations_counter++; }
  // -------------------------------------------------------------------------------------
  return free_bf;
}
// -------------------------------------------------------------------------------------
// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
void BufferManager::reclaimBufferFrame(BufferFrame& bf)
{
  if (bf.header.isWB) {
    // DO NOTHING ! we have a garbage collector ;-)
    bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
    bf.header.latch.mutex.unlock();
    cout << "garbage collector, yeah" << endl;
  } else {
    Partition& partition = getPartition(bf.header.pid);
    bf.reset();
    bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
    bf.header.latch.mutex.unlock();
    partition.dram_free_list.push(bf);
  }
}
// -------------------------------------------------------------------------------------
void BufferManager::reclaimPage(BufferFrame& bf)
{
  // TODO: reclaim bf pid
  ssd_freed_pages_counter++;
  // -------------------------------------------------------------------------------------
  reclaimBufferFrame(bf);
}
// -------------------------------------------------------------------------------------
// returns a non-latched BufferFrame
BufferFrame& BufferManager::resolveSwip(OptimisticGuard& swip_guard,
                                        Swip<BufferFrame>& swip_value)  // throws RestartException
{
  if (swip_value.isSwizzled()) {
    BufferFrame& bf = swip_value.asBufferFrame();
    swip_guard.recheck();
    // -------------------------------------------------------------------------------------
    // TODO: if
    //      debugging_counters.hot_hit_counter++;
    //      PPCounters::thread_local_counters.hot_hit_counter++;
    // -------------------------------------------------------------------------------------
    return bf;
  }
  // -------------------------------------------------------------------------------------
  const PID pid = swip_value.asPageID();
  Partition& partition = getPartition(pid);
  JMUW<std::unique_lock<std::mutex>> g_guard(partition.cio_mutex);
  swip_guard.recheck();
  assert(!swip_value.isSwizzled());
  // -------------------------------------------------------------------------------------
  auto frame_handler = partition.ht.lookup(pid);
  if (!frame_handler) {
    BufferFrame& bf = partition.dram_free_list.tryPop(g_guard);
    CIOFrame& cio_frame = partition.ht.insert(pid);
    assert(bf.header.state == BufferFrame::State::FREE);
    bf.header.latch.assertNotExclusivelyLatched();
    // -------------------------------------------------------------------------------------
    cio_frame.state = CIOFrame::State::READING;
    cio_frame.readers_counter = 1;
    cio_frame.mutex.lock();
    // -------------------------------------------------------------------------------------
    g_guard->unlock();
    // -------------------------------------------------------------------------------------
    readPageSync(pid, bf.page);
    COUNTERS_BLOCK()
    {
      WorkerCounters::myCounters().dt_misses_counter[bf.page.dt_id]++;
      if (FLAGS_trace_dt_id >= 0 && bf.page.dt_id == static_cast<u64>(FLAGS_trace_dt_id) &&
          utils::RandomGenerator::getRand<u64>(0, FLAGS_trace_trigger_probability) == 0) {
        utils::printBackTrace();
      }
    }
    assert(bf.page.magic_debugging_number == pid);
    // -------------------------------------------------------------------------------------
    // ATTENTION: Fill the BF
    assert(!bf.header.isWB);
    bf.header.lastWrittenLSN = bf.page.LSN;
    bf.header.state = BufferFrame::State::COLD;
    bf.header.pid = pid;
    // -------------------------------------------------------------------------------------
    jumpmuTry()
    {
      swip_guard.recheck();
      JMUW<std::unique_lock<std::mutex>> g_guard(partition.cio_mutex);
      ExclusiveGuard swip_x_guard(swip_guard);
      cio_frame.mutex.unlock();
      swip_value.swizzle(&bf);
      bf.header.state = BufferFrame::State::HOT;  // ATTENTION: SET TO HOT AFTER
                                                  // IT IS SWIZZLED IN
      // -------------------------------------------------------------------------------------
      // Simply written, let the compiler optimize it
      bool should_clean = true;
      if (cio_frame.readers_counter.fetch_add(-1) > 1) {
        should_clean = false;
      }
      if (should_clean) {
        partition.ht.remove(pid);
      }
      jumpmu_return bf;
    }
    jumpmuCatch()
    {
      // Move to cooling stage
      g_guard->lock();
      cio_frame.state = CIOFrame::State::COOLING;
      partition.cooling_queue.push_back(&bf);
      cio_frame.fifo_itr = --partition.cooling_queue.end();
      partition.cooling_bfs_counter++;
      // -------------------------------------------------------------------------------------
      bf.header.isCooledBecauseOfReading = true;
      // -------------------------------------------------------------------------------------
      g_guard->unlock();
      cio_frame.mutex.unlock();
      // -------------------------------------------------------------------------------------
      jumpmu::jump();
    }
  }
  // -------------------------------------------------------------------------------------
  CIOFrame& cio_frame = frame_handler.frame();
  // -------------------------------------------------------------------------------------
  if (cio_frame.state == CIOFrame::State::READING) {
    cio_frame.readers_counter++;
    g_guard->unlock();
    cio_frame.mutex.lock();
    cio_frame.mutex.unlock();
    // -------------------------------------------------------------------------------------
    assert(partition.ht.has(pid));
    if (cio_frame.readers_counter.fetch_add(-1) == 1) {
      g_guard->lock();
      if (cio_frame.readers_counter == 0) {
        partition.ht.remove(pid);
      }
      g_guard->unlock();
    }
    // -------------------------------------------------------------------------------------
    jumpmu::jump();
  }
  // -------------------------------------------------------------------------------------
  if (cio_frame.state == CIOFrame::State::COOLING) {
    // -------------------------------------------------------------------------------------
    BufferFrame* bf = *cio_frame.fifo_itr;
    {
      // We have to exclusively lock the bf because the page provider thread will
      // try to evict them when its IO is done
      OptimisticGuard bf_guard(bf->header.latch, FALLBACK_METHOD::SHOULD_NOT_HAPPEN);
      ExclusiveGuard swip_x_guard(swip_guard);
      ExclusiveGuard bf_x_guard(bf_guard);
      // -------------------------------------------------------------------------------------
      assert(bf->header.pid == pid);
      swip_value.swizzle(bf);
      assert(swip_value.isSwizzled());
      partition.cooling_queue.erase(cio_frame.fifo_itr);
      partition.cooling_bfs_counter--;
      assert(bf->header.state == BufferFrame::State::COLD);
      bf->header.state = BufferFrame::State::HOT;  // ATTENTION: SET TO HOT AFTER
                                                   // IT IS SWIZZLED IN
      // -------------------------------------------------------------------------------------
      // Simply written, let the compiler optimize it
      bool should_clean = true;
      if (bf->header.isCooledBecauseOfReading) {
        if (cio_frame.readers_counter.fetch_add(-1) > 1) {
          should_clean = false;
        }
      } else {
        COUNTERS_BLOCK() { WorkerCounters::myCounters().cold_hit_counter++; }
      }
      if (should_clean) {
        partition.ht.remove(pid);
      }
      g_guard->unlock();
    }
    // -------------------------------------------------------------------------------------
    // dt_registry.checkSpaceUtilization(bf->page.dt_id, *bf); // BETA:
    // -------------------------------------------------------------------------------------
    return *bf;
  }
  // it is a bug signal, if the page was hot then we should never hit this path
  UNREACHABLE();
}
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8* destination)
{
  assert(u64(destination) % 512 == 0);
  s64 bytes_left = PAGE_SIZE;
  do {
    const int bytes_read = pread(ssd_fd, destination, bytes_left, pid * PAGE_SIZE + (PAGE_SIZE - bytes_left));
    assert(bytes_left > 0);
    bytes_left -= bytes_read;
  } while (bytes_left > 0);
  // -------------------------------------------------------------------------------------
  COUNTERS_BLOCK() { WorkerCounters::myCounters().read_operations_counter++; }
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
DTID BufferManager::registerDatastructureInstance(DTType type, void* root_object, string name)
{
  DTID new_instance_id = dt_registry.dt_types_ht[type].instances_counter++;
  dt_registry.dt_instances_ht.insert({new_instance_id, {type, root_object, name}});
  // -------------------------------------------------------------------------------------
  COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_misses_counter[new_instance_id] = 0; }
  // -------------------------------------------------------------------------------------
  return new_instance_id;
}
// -------------------------------------------------------------------------------------
// Make sure all worker threads are off
void BufferManager::flushDropAllPages()
{
  // TODO
  // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
u64 BufferManager::getPartitionID(PID pid)
{
  return pid & partitions_mask;
}
// -------------------------------------------------------------------------------------
Partition& BufferManager::getPartition(PID pid)
{
  const u64 partition_i = getPartitionID(pid);
  assert(partition_i < partitions_count);
  return partitions[partition_i];
}
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
  bg_threads_keep_running = false;
  while (bg_threads_counter) {
    MYPAUSE();
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
BufferManager* BMC::global_bf(nullptr);
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
