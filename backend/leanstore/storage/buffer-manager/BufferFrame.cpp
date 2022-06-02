#include "BufferFrame.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
atomic<WATT_TIME> BufferFrame::globalTrackerTime =1;
BufferFrame::Header::WATT_LOG BufferFrame::Header::watt_backlog = WATT_LOG();
// -------------------------------------------------------------------------------------
}
}  // namespace leanstore
   // -------------------------------------------------------------------------------------