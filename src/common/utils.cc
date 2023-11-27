#include "common/utils.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "leanstore/config.h"

#include "share_headers/crc.h"
#include "share_headers/logger.h"

#include <sys/ioctl.h>
#include <atomic>
#include <cassert>
#include <cstring>
#include <span>
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

auto ComputeCRC(const u8 *src, u64 size) -> u32 { return CRC::Calculate(src, size, CRC::CRC_32()); }

// Align utilities
auto UpAlign(u64 x) -> u64 { return (x + (BLOCK_ALIGNMENT_MASK)) & ~BLOCK_ALIGNMENT_MASK; }

auto DownAlign(u64 x) -> u64 { return x - (x & BLOCK_ALIGNMENT_MASK); }

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

template u32 LoadUnaligned<u32>(void *p);
template int LoadUnaligned<int>(void *p);
template pageid_t LoadUnaligned<pageid_t>(void *p);
template leng_t LoadUnaligned<leng_t>(void *p);
template void UpdateMax<u64>(std::atomic<u64> &, u64);

}  // namespace leanstore