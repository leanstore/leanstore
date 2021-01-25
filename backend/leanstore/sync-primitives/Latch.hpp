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
namespace storage
{
#define MAX_BACKOFF FLAGS_backoff  // FLAGS_x
#define BACKOFF_STRATEGIES()                                              \
   if (FLAGS_backoff) {                                                   \
      for (u64 i = utils::RandomGenerator::getRandU64(0, mask); i; --i) { \
         MYPAUSE();                                                       \
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
enum class LATCH_FALLBACK_MODE : u8 { SHARED = 0, EXCLUSIVE = 1, JUMP = 2, SPIN = 3, SHOULD_NOT_HAPPEN = 4 };
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
   Guard(HybridLatch& latch, GUARD_STATE state) : latch(&latch), state(state), version(latch.ref().load()) {}
   // -------------------------------------------------------------------------------------
   // Move
   Guard(Guard&& other) : latch(other.latch), state(other.state), version(other.version), faced_contention(other.faced_contention)
   {
      other.state = GUARD_STATE::MOVED;
   }
   Guard& operator=(Guard&& other)
   {
      unlock();
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
      // maybe only if state == optimistic
      assert(state == GUARD_STATE::OPTIMISTIC || version == latch->ref().load());
      if (state == GUARD_STATE::OPTIMISTIC && version != latch->ref().load()) {
         jumpmu::jump();
      }
   }
   // -------------------------------------------------------------------------------------
   inline void unlock()
   {
      if (state == GUARD_STATE::EXCLUSIVE) {
         version += LATCH_EXCLUSIVE_BIT;
         latch->ref().store(version, std::memory_order_release);
         latch->mutex.unlock();
         state = GUARD_STATE::OPTIMISTIC;
      } else if (state == GUARD_STATE::SHARED) {
         latch->mutex.unlock_shared();
         state = GUARD_STATE::OPTIMISTIC;
      }
   }
   // -------------------------------------------------------------------------------------
   inline void toOptimisticSpin()
   {
      assert(state == GUARD_STATE::UNINITIALIZED && latch != nullptr && state != GUARD_STATE::MOVED);
      version = latch->ref().load();
      if ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
         faced_contention = true;
         do {
            version = latch->ref().load();
         } while ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
      }
      state = GUARD_STATE::OPTIMISTIC;
   }
   inline void toOptimisticOrJump()
   {
      assert(state == GUARD_STATE::UNINITIALIZED && latch != nullptr && state != GUARD_STATE::MOVED);
      version = latch->ref().load();
      if ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
         jumpmu::jump();
      } else {
         state = GUARD_STATE::OPTIMISTIC;
      }
   }
   inline void toOptimisticOrShared()
   {
      assert(state == GUARD_STATE::UNINITIALIZED && latch != nullptr && state != GUARD_STATE::MOVED);
      version = latch->ref().load();
      if ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
         latch->mutex.lock_shared();
         version = latch->ref().load();
         state = GUARD_STATE::SHARED;
         faced_contention = true;
      } else {
         state = GUARD_STATE::OPTIMISTIC;
      }
   }
   inline void toOptimisticOrExclusive()
   {
      assert(state == GUARD_STATE::UNINITIALIZED && latch != nullptr && state != GUARD_STATE::MOVED);
      version = latch->ref().load();
      if ((version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
         latch->mutex.lock();
         version = latch->ref().load() + LATCH_EXCLUSIVE_BIT;
         latch->ref().store(version, std::memory_order_release);
         state = GUARD_STATE::EXCLUSIVE;
         faced_contention = true;
      } else {
         state = GUARD_STATE::OPTIMISTIC;
      }
   }
   inline void toExclusive()
   {
      assert(state != GUARD_STATE::SHARED);
      if (state == GUARD_STATE::EXCLUSIVE)
         return;
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
      assert(state == GUARD_STATE::OPTIMISTIC || state == GUARD_STATE::SHARED);
      if (state == GUARD_STATE::SHARED)
         return;
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
         latch->mutex.lock_shared();
         version = latch->ref().load();
         state = GUARD_STATE::SHARED;
      }
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
