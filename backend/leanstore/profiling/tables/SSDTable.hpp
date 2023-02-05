#pragma once
#include "ProfilingTable.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
class SSDTable : public ProfilingTable
{
  private:
     int ssd;
     u64 local_pushed;
     u64 local_polled;

  public:
   SSDTable();
   // -------------------------------------------------------------------------------------
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
