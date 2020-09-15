#pragma once
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#ifdef __x86_64__
#include <emmintrin.h>
#define MYPAUSE() _mm_pause()
#endif
#ifdef __aarch64__
#include <arm_acle.h>
#define MYPAUSE() asm("YIELD");
#endif
#include <unistd.h>

#include <atomic>
#include <shared_mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
#define MAX_BACKOFF FLAGS_backoff  // FLAGS_x
#define BACKOFF_STRATEGIES()                                            \
  if (FLAGS_backoff) {                                                  \
    for (u64 i = utils::RandomGenerator::getRandU64(0, mask); i; --i) { \
      MYPAUSE();                                                        \
    }                                                                   \
    mask = mask < MAX_BACKOFF ? mask << 1 : MAX_BACKOFF;                \
  }

// -------------------------------------------------------------------------------------
struct RestartException {
 public:
  RestartException() {}
};
// -------------------------------------------------------------------------------------
constexpr static u64 LATCH_EXCLUSIVE_BIT = 1ull;
constexpr static u64 LATCH_VERSION_MASK = ~(0ull);
// -------------------------------------------------------------------------------------
using VersionType = atomic<u64>;
struct alignas(64) HybridLatch {
  VersionType version;
  std::shared_mutex mutex;
  // -------------------------------------------------------------------------------------
  template <typename... Args>
  HybridLatch(Args&&... args) : version(std::forward<Args>(args)...)
  {
  }
  VersionType* operator->() { return &version; }
  // -------------------------------------------------------------------------------------
  VersionType* ptr() { return &version; }
  VersionType& ref() { return version; }
  // -------------------------------------------------------------------------------------
  void assertExclusivelyLatched() { assert(isExclusivelyLatched()); }
  void assertNotExclusivelyLatched() { assert(!isExclusivelyLatched()); }
  // -------------------------------------------------------------------------------------
  bool isExclusivelyLatched() { return (version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT; }
};
static_assert(sizeof(HybridLatch) == 64, "");
// -------------------------------------------------------------------------------------
enum class GUARD_STATE { UNINITIALIZED, OPTIMISTIC, SHARED, EXCLUSIVE, MOVED };
enum class FALLBACK_METHOD { JUMP, SPIN, SHARED, EXCLUSIVE, SHOULD_NOT_HAPPEN };
// -------------------------------------------------------------------------------------
struct Guard {
  HybridLatch* latch = nullptr;
  GUARD_STATE state = GUARD_STATE::UNINITIALIZED;
  u64 version;
  bool faced_contention = false;
  // -------------------------------------------------------------------------------------
  Guard(HybridLatch& latch) : latch(&latch) { transition(GUARD_STATE::OPTIMISTIC); }
  Guard(HybridLatch* latch) : latch(latch) { transition(GUARD_STATE::OPTIMISTIC); }
  // -------------------------------------------------------------------------------------
  Guard(HybridLatch& latch, GUARD_STATE state, u64 version) : latch(&latch), state(state), version(version) {}
  // -------------------------------------------------------------------------------------
  // Move
  Guard(Guard&& other) : latch(other.latch), state(other.state), version(other.version) { other.state = GUARD_STATE::MOVED; }
  Guard& operator=(Guard&& other)
  {
    transition(GUARD_STATE::OPTIMISTIC);
    // -------------------------------------------------------------------------------------
    latch = other.latch;
    state = other.state;
    version = other.version;
    // -------------------------------------------------------------------------------------
    other.state = GUARD_STATE::MOVED;
    // -------------------------------------------------------------------------------------
    return *this;
  }
  // -------------------------------------------------------------------------------------
  void recheck()
  {
    if (version != latch->ref().load()) {
      jumpmu::jump();
    }
  }
  // -------------------------------------------------------------------------------------
  void transition(GUARD_STATE dest_state, FALLBACK_METHOD if_contended = FALLBACK_METHOD::SPIN)
  {
    // -------------------------------------------------------------------------------------
    // enum class GUARD_STATE { UNINITIALIZED, OPTIMISTIC, SHARED, EXCLUSIVE, MOVED, RELEASED };
    if (state == dest_state || state == GUARD_STATE::MOVED || latch == nullptr) {
      return;
    }
    switch (state) {
      case GUARD_STATE::UNINITIALIZED: {
        ensure(dest_state == GUARD_STATE::OPTIMISTIC);
        for (u32 attempt = 0; attempt < 40; attempt++) {
          version = latch->ref().load();
          if (((version & LATCH_EXCLUSIVE_BIT) == 0)) {
            state = GUARD_STATE::OPTIMISTIC;
            return;
          }
        }
        switch (if_contended) {
          case FALLBACK_METHOD::SPIN: {
            volatile u32 mask = 1;
            while ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
              BACKOFF_STRATEGIES()
              version = latch->ref().load();
            }
            state = GUARD_STATE::OPTIMISTIC;
            break;
          }
          case FALLBACK_METHOD::JUMP: {
            jumpmu::jump();
          }
          case FALLBACK_METHOD::EXCLUSIVE: {
            latch->mutex.lock();
            version = latch->ref().load() + LATCH_EXCLUSIVE_BIT;
            latch->ref().store(version);
            state = GUARD_STATE::EXCLUSIVE;
            faced_contention = true;
            break;
          }
          case FALLBACK_METHOD::SHARED: {
            latch->mutex.lock_shared();
            version = latch->ref().load();
            state = GUARD_STATE::SHARED;
            faced_contention = true;
            break;
          }
          default:
            ensure(false);
        }
        break;
      }
      case GUARD_STATE::OPTIMISTIC: {
        if (dest_state == GUARD_STATE::EXCLUSIVE) {
          const u64 new_version = version + LATCH_EXCLUSIVE_BIT;
          u64 expected = version;
          if (!latch->mutex.try_lock()) {
            jumpmu::jump();
          }
          if (!latch->ref().compare_exchange_strong(expected, new_version)) {
            latch->mutex.unlock();
            jumpmu::jump();
          }
          version = new_version;
          state = GUARD_STATE::EXCLUSIVE;
        } else if (dest_state == GUARD_STATE::SHARED) {
          if (!latch->mutex.try_lock_shared()) {
            jumpmu::jump();
          }
          if (latch->ref().load() != version) {
            jumpmu::jump();
          }
          state = GUARD_STATE::SHARED;
        } else {
          ensure(false);
        }
        break;
      }
      case GUARD_STATE::SHARED: {
        if (dest_state == GUARD_STATE::SHARED) {
          break;
        }
        ensure(dest_state == GUARD_STATE::OPTIMISTIC);
        latch->mutex.unlock_shared();
        state = GUARD_STATE::OPTIMISTIC;
        break;
      }
      case GUARD_STATE::EXCLUSIVE: {
        if (dest_state == GUARD_STATE::EXCLUSIVE) {
          break;
        }
        ensure(dest_state == GUARD_STATE::OPTIMISTIC);
        version += LATCH_EXCLUSIVE_BIT;
        latch->ref().store(version);
        latch->mutex.unlock();
        state = GUARD_STATE::OPTIMISTIC;
        break;
      }
      default:
        ensure(false);
        break;
    }
  }
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
