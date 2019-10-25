#include "SmartPointer.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
SmartPointer::SmartPointer(leanstore::Swip &swip)
        : swip(swip)
{
   while ( true ) {
      try {
         bf = &BMC::global_bf->fixPage(swip);
         lock = SharedLock(bf->header.lock);
      } catch ( OptimisticLockException e ) {
      }
   }
}
}