#include "SmartPointer.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
SmartPointer::SmartPointer(BufferFrame &swip_holder, leanstore::Swip &swip)
        : swip_holder(swip_holder), swip(swip)
{
   while ( true ) {
      try {
         bf = &BMC::global_bf->fixPage(swip_holder,swip);
         lock = SharedLock(bf->header.lock);
         break;
      } catch ( RestartException e ) {
      }
   }
}
}