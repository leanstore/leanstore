#pragma once
#include "ProfilingTable.hpp"
#include "leanstore/profiling/counters/ThreadCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
class ThreadTable : public ProfilingTable
{
  private:
     ThreadCounters* counter = nullptr;

  public:
   ThreadTable();
   // -------------------------------------------------------------------------------------
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
