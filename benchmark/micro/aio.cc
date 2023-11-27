#include "benchmark/benchmark.h"
#include "libaio.h"
#include "liburing.h"

#include <fcntl.h>
#include <filesystem>
#include <thread>

#define QUEUE_DEPTH 64

static void BM_io_uring(benchmark::State &state) {
  struct io_uring ring;
  uint8_t data[state.range(1)] = {0};
  auto test_file               = std::filesystem::temp_directory_path() / std::filesystem::path("aio-test");
  auto fd                      = open(test_file.c_str(), O_RDWR | O_DIRECT | O_CREAT, S_IRWXU);

  [[maybe_unused]] int ret = io_uring_queue_init(QUEUE_DEPTH, &ring, IORING_SETUP_IOPOLL);
  assert(ret >= 0);

  for (auto _ : state) {
    auto no_req    = state.range(0);
    auto page_size = state.range(1);

    int submit_idx = 0;
    while (submit_idx < no_req) {
      size_t submit_size = 0;
      for (; submit_size < std::min<size_t>(no_req - submit_idx, QUEUE_DEPTH); submit_size++) {
        auto pid = submit_idx + submit_size;
        auto sqe = io_uring_get_sqe(&ring);
        io_uring_prep_write(sqe, fd, data, page_size, page_size * pid);
      }
      [[maybe_unused]] auto ret = io_uring_submit(&ring);
      assert(ret == static_cast<int>(submit_size));
      struct io_uring_cqe *cqes[submit_size];
      io_uring_wait_cqe_nr(&ring, cqes, submit_size);
      io_uring_cq_advance(&ring, submit_size);
      submit_idx += submit_size;
    }
  }

  io_uring_queue_exit(&ring);
}

static void BM_libaio(benchmark::State &state) {
  io_context_t ctx;
  uint8_t data[state.range(1)] = {0};
  auto test_file               = std::filesystem::temp_directory_path() / std::filesystem::path("aio-test");
  auto fd                      = open(test_file.c_str(), O_RDWR | O_DIRECT | O_CREAT, S_IRWXU);

  memset(&ctx, 0, sizeof(io_context_t));
  [[maybe_unused]] int ret = io_setup(state.range(0), &ctx);
  assert(ret >= 0);

  for (auto _ : state) {
    auto no_req    = state.range(0);
    auto page_size = state.range(1);

    iocb cb[no_req];
    iocb *cb_ptr[no_req];
    struct io_event events[no_req];

    int submit_idx = 0;
    while (submit_idx < no_req) {
      size_t idx = 0;
      for (; idx < std::min<size_t>(no_req - submit_idx, QUEUE_DEPTH); idx++) {
        auto pid    = submit_idx + idx;
        cb_ptr[idx] = &cb[idx];
        io_prep_pwrite(&cb[idx], fd, data, page_size, page_size * pid);
      }
      [[maybe_unused]] int cnt = io_submit(ctx, idx, cb_ptr);
      assert(static_cast<size_t>(cnt) == idx);
      cnt = io_getevents(ctx, idx, idx, events, nullptr);
      assert(static_cast<size_t>(cnt) == idx);
      submit_idx += idx;
    }
  }

  io_destroy(ctx);
}

BENCHMARK(BM_io_uring)
  ->Args({100, 4096})
  ->Args({1000, 4096})
  ->Args({10000, 4096})
  ->Args({10, 40960})
  ->Args({100, 40960})
  ->Args({1000, 40960});
BENCHMARK(BM_libaio)
  ->Args({100, 4096})
  ->Args({1000, 4096})
  ->Args({10000, 4096})
  ->Args({10, 40960})
  ->Args({100, 40960})
  ->Args({1000, 40960});

BENCHMARK_MAIN();