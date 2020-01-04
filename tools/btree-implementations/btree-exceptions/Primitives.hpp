#pragma once
#include <unistd.h>

#include <atomic>

#include "Units.hpp"
using namespace std;
namespace btree
{
namespace libgcc
{
// -------------------------------------------------------------------------------------
struct OptimisticLockException {
 public:
  OptimisticLockException() {}
  OptimisticLockException(int code) { cout << code << endl; }
};
// -------------------------------------------------------------------------------------
class SharedLock;
class ExclusiveLock;
using lock_version_t = u64;
using lock_t = atomic<lock_version_t>;
// -------------------------------------------------------------------------------------
class SharedLock
{
  friend class ExclusiveLock;

 private:
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
    while ((local_version & 2) == 2) {  // spin bf_s_lock
      usleep(5);
      local_version = version_ptr->load();
    }
    locked = true;
  }
  // -------------------------------------------------------------------------------------
  void recheck()
  {
    if (locked && local_version != *version_ptr) {
      throw OptimisticLockException();
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
 private:
  SharedLock& ref_lock;  // our basis
 public:
  // -------------------------------------------------------------------------------------
  ExclusiveLock(SharedLock& shared_lock) : ref_lock(shared_lock)
  {
    assert(ref_lock.version_ptr != nullptr);
    lock_version_t new_version = ref_lock.local_version + 2;
    if (!std::atomic_compare_exchange_strong(ref_lock.version_ptr, &ref_lock.local_version, new_version)) {
      throw OptimisticLockException();
    }
    ref_lock.local_version = new_version;
  }
  // -------------------------------------------------------------------------------------
  ~ExclusiveLock()
  {
    assert(ref_lock.version_ptr != nullptr);
    if (ref_lock.version_ptr != nullptr) {
      ref_lock.local_version = 2 + ref_lock.version_ptr->fetch_add(2);
    }
  }
};
// -------------------------------------------------------------------------------------
}  // namespace libgcc
}  // namespace btree
