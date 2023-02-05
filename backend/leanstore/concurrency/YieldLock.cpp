// -------------------------------------------------------------------------------------
#include "YieldLock.hpp"
// -------------------------------------------------------------------------------------
#include "Mean.hpp"
#include "Task.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <mutex>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
bool YieldLock::try_lock()
{
   bool b = !_lock.test_and_set(std::memory_order_acquire);
   if (b) {
      _owner = mean::exec::getId();
   }
   return b;
}
void YieldLock::lock()
{
   if (!try_lock()) {
      _waiting++;
      /*
      if(_waiting > 50)
         raise(SIGINT);
         */
      auto& this_task = mean::task::this_task();
      this_task.lock = this;
      mean::task::yield(mean::TaskState::ReadyLock);
      // must be locked at this point
      _waiting--;
   }
}
void YieldLock::unlock()
{
   _owner = -1;
   _lock.clear(std::memory_order_release);
}
}  // namespace mean
// -------------------------------------------------------------------------------------
