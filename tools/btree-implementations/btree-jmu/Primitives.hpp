#pragma once
#include <signal.h>

#include <atomic>

#include "JumpMU.hpp"
#include "Units.hpp"
using namespace std;
namespace btree
{
namespace jmu
{
// -------------------------------------------------------------------------------------
class SharedLock;
class ExclusiveLock;
using lock_version_t = u64;
using lock_t = atomic<lock_version_t>;
// -------------------------------------------------------------------------------------
class SharedLock
{
  friend class ExclusiveLock;

 public:
  atomic<u64>* version_ptr = nullptr;
  u64 local_version;
  bool locked = false;

 public:
  // -------------------------------------------------------------------------------------
  SharedLock() = default;
  // -------------------------------------------------------------------------------------
  SharedLock(lock_t& lock) : version_ptr(&lock)
  {
    local_version = version_ptr->load();
    while ((local_version & 1) == 1) {  // spin bf_s_lock
      usleep(5);
      local_version = version_ptr->load();
    }
    locked = true;
  }
  // -------------------------------------------------------------------------------------
  void recheck()
  {
    if (locked && local_version != *version_ptr) {
      jumpmu::restore();
    }
  }
  // -------------------------------------------------------------------------------------
  SharedLock& operator=(const SharedLock& other) = default;
  operator bool() const { return locked; }
  // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class ExclusiveLock
{
 public:
  SharedLock& ref_lock;  // our basis
 public:
  // -------------------------------------------------------------------------------------
  ExclusiveLock(SharedLock& shared_lock) : ref_lock(shared_lock)
  {
    assert(ref_lock.version_ptr != nullptr);
    assert((shared_lock.local_version & 1) == 0);
    lock_version_t new_version = ref_lock.local_version + 1;
    if (!std::atomic_compare_exchange_strong(ref_lock.version_ptr, &ref_lock.local_version, new_version)) {
      assert(jumpmu::checkpoint_counter == 1);
      jumpmu::restore();
    }
    ref_lock.local_version = new_version;
    assert((ref_lock.local_version & 1) == 1);

    jumpmu_registerDestructor();
    assert(jumpmu::checkpoint_counter == 1);
    assert(jumpmu::de_stack_counter <= 2);
  }
  // -------------------------------------------------------------------------------------
  jumpmu_defineCustomDestructor(ExclusiveLock) ~ExclusiveLock()
  {
    assert(ref_lock.version_ptr != nullptr);
    if (ref_lock.version_ptr != nullptr) {
      ref_lock.local_version = 1 + ref_lock.version_ptr->fetch_add(1);
    }
    assert((ref_lock.local_version & 1) == 0);
    assert(jumpmu::checkpoint_counter <= 1);
    assert(jumpmu::de_stack_counter <= 2);
    jumpmu::decrement();
  }
};
// -------------------------------------------------------------------------------------

}  // namespace jmu
}  // namespace btree
