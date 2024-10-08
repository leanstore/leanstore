#include "common/utils.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "leanstore/config.h"

#include "fmt/format.h"
#include "share_headers/crc.h"
#include "share_headers/logger.h"

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <x86intrin.h>
#include <atomic>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <ranges>
#include <span>
#include <string_view>
#include <vector>

namespace leanstore {

auto AllocHuge(size_t size) -> void * {
  void *p = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  madvise(p, size, MADV_HUGEPAGE);
  return p;
}

auto HashFn(u64 k) -> u64 {
  const u64 m = 0xc6a4a7935bd1e995;
  const int r = 47;
  u64 h       = 0x8445d61a4e774912 ^ (8 * m);
  k *= m;
  k ^= k >> r;
  k *= m;
  h ^= k;
  h *= m;
  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

void WarningMessage(const std::string &msg) { std::cout << "\033[1;31m" << msg << "\n\033[0m"; }

/**
 * @brief Submit `submit_cnt` uring requests and wait for their completion
 * Will reset `submit_cnt` back to 0 at the end
 */
void UringSubmit(struct io_uring *ring, u32 submit_cnt, const std::function<void()> &fn) {
  if (submit_cnt > 0) {
    [[maybe_unused]] auto ret = io_uring_submit(ring);
    assert(ret == static_cast<int>(submit_cnt));

    /* Whether the caller wants to trigger anything during log flush */
    if (fn) { fn(); };

    while (submit_cnt > 0) {
      struct io_uring_cqe *cqes[submit_cnt];
      auto count = io_uring_peek_batch_cqe(ring, cqes, submit_cnt);
#ifdef DEBUG
      for (auto idx = 0UL; idx < count; idx++) { assert(cqes[idx]->res >= 0); }
#endif
      if (count == 0) {
        // enter kernel for polling
        [[maybe_unused]] int submitted = io_uring_submit(ring);
        assert(submitted == 0);
      } else {
        io_uring_cq_advance(ring, count);
        submit_cnt -= count;
      }
    }
  }
}

auto ListFilesWithExt(const std::string &directory, const std::string &ext) -> std::vector<std::string> {
  std::vector<std::string> result;
  for (const auto &entry : std::filesystem::directory_iterator(directory)) {
    if (entry.is_regular_file() && entry.path().extension() == ext) { result.emplace_back(entry.path().string()); }
  }
  return result;
}

template <typename T>
void WriteSequenceToFile(const T &sequence, u32 sample_ratio, const std::string &filename) {
  std::ofstream myfile(filename);
  for (auto idx = 1U; idx <= sample_ratio; idx++) {
    auto pos = std::min(static_cast<size_t>(sequence.size() * static_cast<float>(idx) / sample_ratio), sequence.size());
    myfile << sequence[pos - 1] << std::endl;
  }
  myfile.close();
}

auto ComputeCRC(const u8 *src, u64 size) -> u32 { return CRC::Calculate(src, size, CRC::CRC_32()); }

// Align utilities
auto UpAlign(u64 x, u64 align_size) -> u64 {
  assert((align_size & (align_size - 1)) == 0);
  return (x + (align_size - 1)) & ~(align_size - 1);
}

auto DownAlign(u64 x, u64 align_size) -> u64 {
  assert((align_size & (align_size - 1)) == 0);
  return x - (x & (align_size - 1));
}

void PinThisThread(wid_t t_i) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(t_i, &cpuset);
  pthread_t current_thread = pthread_self();
  if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
    throw leanstore::ex::GenericException("Could not pin a thread, maybe because of over subscription?");
  }
}

auto IsAligned(u64 align_size, const void *p, size_t p_size) -> bool {
  return ((reinterpret_cast<u64>(p) % align_size == 0) && (p_size % align_size == 0));
}

auto ExmapAction(int exmapfd, exmap_opcode op, leng_t len) -> int {
  struct exmap_action_params params_free = {
    .interface = static_cast<u16>(worker_thread_id),
    .iov_len   = len,
    .opcode    = static_cast<u16>(op),
    .flags     = 0,
  };
  return ioctl(exmapfd, EXMAP_IOCTL_ACTION, &params_free);
}

auto PageTableSize() -> std::string {
  auto pid = getpid();
  std::ifstream proc_status(fmt::format("/proc/{}/status", pid).c_str());
  std::string line;
  while (true) {
    std::getline(proc_status, line);
    if (line.empty()) { break; }
    if (line.starts_with("VmPTE:")) {
      auto pos = line.rfind(' ');
      Ensure(pos != line.npos);
      pos = line.substr(0, pos).rfind(' ');
      return line.substr(pos + 1);
    }
  }
  return {};
}

auto StorageCapacity(const char *path) -> u64 {
  int fd = open(path, 0, 0666);
  size_t storage_size;
  int rc = ioctl(fd, BLKGETSIZE64, &storage_size);
  Ensure(rc == 0);
  return storage_size;
}

// ---------------------------------------------------------------------------
// Stolen from Folly
// ---------------------------------------------------------------------------

void AsmBarrier() {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : : : "memory");
#elif defined(_MSC_VER)
  ::_ReadWriteBarrier();
#endif
}

void AsmYield([[maybe_unused]] u64 counter) {
#if defined(__i386__) || (defined(__mips_isa_rev) && __mips_isa_rev > 1)
  asm volatile("pause");
#elif (defined(__aarch64__) && !(__ARM_ARCH < 7))
  asm volatile("yield");
#else
  ::_mm_pause();
#endif
}

/**
 * @brief Get order-preserving head of key (assuming little-endian)
 */
template <class T>
auto LoadUnaligned(void *p) -> T {
  T x;
  std::memcpy(&x, p, sizeof(T));
  return x;
}

template <typename T>
void UpdateMax(std::atomic<T> &atomic_val, T value) {
  auto prev_value = atomic_val.load();
  while (prev_value < value && !atomic_val.compare_exchange_weak(prev_value, value)) {}
}

template <typename T>
void UpdateMin(std::atomic<T> &atomic_val, T value) {
  auto prev_value = atomic_val.load();
  while (prev_value > value && !atomic_val.compare_exchange_weak(prev_value, value)) {}
}

template void WriteSequenceToFile<std::vector<timestamp_t>>(const std::vector<timestamp_t> &sequence, u32 sample_ratio,
                                                            const std::string &filename);
template u32 LoadUnaligned<u32>(void *p);
template int LoadUnaligned<int>(void *p);
template pageid_t LoadUnaligned<pageid_t>(void *p);
template leng_t LoadUnaligned<leng_t>(void *p);
template void UpdateMax<u64>(std::atomic<u64> &, u64);
template void UpdateMin<u64>(std::atomic<u64> &, u64);

}  // namespace leanstore