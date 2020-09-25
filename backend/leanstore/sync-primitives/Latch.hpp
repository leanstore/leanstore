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
  Guard(HybridLatch& latch) : latch(&latch) {}
  Guard(HybridLatch* latch) : latch(latch) {}
  // -------------------------------------------------------------------------------------
  Guard(HybridLatch& latch, GUARD_STATE state, u64 version) : latch(&latch), state(state), version(version) {}
  // -------------------------------------------------------------------------------------
  // Move
  Guard(Guard&& other) : latch(other.latch), state(other.state), version(other.version), faced_contention(other.faced_contention)
  {
    other.state = GUARD_STATE::MOVED;
  }
  Guard& operator=(Guard&& other)
  {
    toOptimisticSpin();
    // -------------------------------------------------------------------------------------
    latch = other.latch;
    state = other.state;
    version = other.version;
    faced_contention = other.faced_contention;
    // -------------------------------------------------------------------------------------
    other.state = GUARD_STATE::MOVED;
    // -------------------------------------------------------------------------------------
    return *this;
  }
  // -------------------------------------------------------------------------------------
  void recheck()
  {
    assert(state == GUARD_STATE::OPTIMISTIC || state == GUARD_STATE::EXCLUSIVE || state == GUARD_STATE::SHARED);
    if (state == GUARD_STATE::OPTIMISTIC) {
      if (version != latch->ref().load()) {
        jumpmu::jump();
      }
    }
  }
  // -------------------------------------------------------------------------------------
  inline void toOptimisticSpin()
  {
    if (state == GUARD_STATE::OPTIMISTIC || state == GUARD_STATE::MOVED || latch == nullptr) {
      return;
    }
    if (state == GUARD_STATE::UNINITIALIZED) {
      volatile u32 mask = 1;
      version = latch->ref().load();
      while ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
        for (u64 i = utils::RandomGenerator::getRandU64(0, mask); i; --i) {
          MYPAUSE();
        }
        if (mask < MAX_BACKOFF) {
          mask = mask << 1;
        } else {
          mask = MAX_BACKOFF;
          faced_contention = true;
        }
        version = latch->ref().load();
      }
    } else if (state == GUARD_STATE::EXCLUSIVE) {
      version += LATCH_EXCLUSIVE_BIT;
      latch->ref().store(version, std::memory_order_release);
      latch->mutex.unlock();
    } else if (state == GUARD_STATE::SHARED) {
      latch->mutex.unlock_shared();
    }
    state = GUARD_STATE::OPTIMISTIC;
  }
  inline void toOptimisticOrJump()
  {
    if (state == GUARD_STATE::OPTIMISTIC || state == GUARD_STATE::MOVED || latch == nullptr) {
      return;
    }
    for (u8 attempt = 0; attempt < 40; attempt++) {
      version = latch->ref().load();
      if ((version & LATCH_EXCLUSIVE_BIT) == 0) {
        state = GUARD_STATE::OPTIMISTIC;
        return;
      }
    }
    jumpmu::jump();
  }
  inline void toOptimisticOrShared()
  {
    if (state == GUARD_STATE::OPTIMISTIC || state == GUARD_STATE::MOVED || latch == nullptr) {
      return;
    }
    for (u8 attempt = 0; attempt < 40; attempt++) {
      version = latch->ref().load();
      if ((version & LATCH_EXCLUSIVE_BIT) == 0) {
        state = GUARD_STATE::OPTIMISTIC;
        return;
      }
    }
    latch->mutex.lock_shared();
    version = latch->ref().load();
    state = GUARD_STATE::SHARED;
    faced_contention = true;
  }
  inline void toOptimisticOrExclusive()
  {
    if (state == GUARD_STATE::OPTIMISTIC || state == GUARD_STATE::MOVED || latch == nullptr) {
      return;
    }
    for (u8 attempt = 0; attempt < 40; attempt++) {
      version = latch->ref().load();
      if ((version & LATCH_EXCLUSIVE_BIT) == 0) {
        state = GUARD_STATE::OPTIMISTIC;
        return;
      }
    }
    latch->mutex.lock();
    version = latch->ref().load() + LATCH_EXCLUSIVE_BIT;
    latch->ref().store(version, std::memory_order_release);
    state = GUARD_STATE::EXCLUSIVE;
    faced_contention = true;
  }
  inline void toExclusive()
  {
    if (state == GUARD_STATE::SHARED || state == GUARD_STATE::MOVED || latch == nullptr) {
      return;
    }
    if (state == GUARD_STATE::OPTIMISTIC) {
      const u64 new_version = version + LATCH_EXCLUSIVE_BIT;
      u64 expected = version;
      latch->mutex.lock();  // changed from try_lock because of possible retries b/c lots of readers
      if (!latch->ref().compare_exchange_strong(expected, new_version)) {
        latch->mutex.unlock();
        jumpmu::jump();
      }
      version = new_version;
      state = GUARD_STATE::EXCLUSIVE;
    } else {
      latch->mutex.lock();
      version = latch->ref().load() + LATCH_EXCLUSIVE_BIT;
      latch->ref().store(version, std::memory_order_release);
      state = GUARD_STATE::EXCLUSIVE;
    }
  }
  inline void toShared()
  {
    if (state == GUARD_STATE::SHARED || state == GUARD_STATE::MOVED || latch == nullptr) {
      return;
    }
    if (state == GUARD_STATE::OPTIMISTIC) {
      if (!latch->mutex.try_lock_shared()) {
        jumpmu::jump();
      }
      if (latch->ref().load() != version) {
        latch->mutex.unlock_shared();
        jumpmu::jump();
      }
      state = GUARD_STATE::SHARED;
    } else {
      assert(false);
    }
  }
  // -------------------------------------------------------------------------------------
  // Must release mutex before jumping
  template <GUARD_STATE dest_state, FALLBACK_METHOD if_contended = FALLBACK_METHOD::SPIN>
  inline void transition()
  {
    if (state == dest_state || state == GUARD_STATE::MOVED || latch == nullptr) {
      return;
    }
    switch (state) {
      case GUARD_STATE::UNINITIALIZED: {
        if (dest_state == GUARD_STATE::OPTIMISTIC) {
          for (u8 attempt = 0; attempt < 40; attempt++) {
            version = latch->ref().load();
            if ((version & LATCH_EXCLUSIVE_BIT) == 0) {
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
              faced_contention = true;
              break;
            }
            case FALLBACK_METHOD::JUMP: {
              jumpmu::jump();
              break;
            }
            case FALLBACK_METHOD::EXCLUSIVE: {
              latch->mutex.lock();
              version = latch->ref().load() + LATCH_EXCLUSIVE_BIT;
              latch->ref().store(version, std::memory_order_release);
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
        } else if (dest_state == GUARD_STATE::EXCLUSIVE) {
          latch->mutex.lock();
          version = latch->ref().load() + LATCH_EXCLUSIVE_BIT;
          latch->ref().store(version, std::memory_order_release);
          state = GUARD_STATE::EXCLUSIVE;
        } else if (dest_state == GUARD_STATE::SHARED) {
          latch->mutex.lock_shared();
          version = latch->ref().load();
          state = GUARD_STATE::SHARED;
        }
        break;
      }
      case GUARD_STATE::OPTIMISTIC: {
        if (dest_state == GUARD_STATE::EXCLUSIVE) {
          const u64 new_version = version + LATCH_EXCLUSIVE_BIT;
          u64 expected = version;
          latch->mutex.lock();  // changed from try_lock because of possible retries b/c lots of readers
          if (!latch->ref().compare_exchange_strong(expected, new_version)) {
            latch->mutex.unlock();
            jumpmu::jump();
          }
          version = new_version;
          state = GUARD_STATE::EXCLUSIVE;
          // -------------------------------------------------------------------------------------
        } else if (dest_state == GUARD_STATE::SHARED) {
          if (!latch->mutex.try_lock_shared()) {
            jumpmu::jump();
          }
          if (latch->ref().load() != version) {
            latch->mutex.unlock_shared();
            jumpmu::jump();
          }
          state = GUARD_STATE::SHARED;
        } else {
          ensure(false);
        }
        break;
      }
      case GUARD_STATE::SHARED: {
        latch->mutex.unlock_shared();
        state = GUARD_STATE::OPTIMISTIC;
        // -------------------------------------------------------------------------------------
        if (dest_state == GUARD_STATE::EXCLUSIVE) {
          transition<dest_state, if_contended>();
        }
        break;
      }
      case GUARD_STATE::EXCLUSIVE: {
        version += LATCH_EXCLUSIVE_BIT;
        latch->ref().store(version, std::memory_order_release);
        latch->mutex.unlock();
        state = GUARD_STATE::OPTIMISTIC;
        // -------------------------------------------------------------------------------------
        if (dest_state == GUARD_STATE::SHARED) {
          transition<dest_state, if_contended>();
        }
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
