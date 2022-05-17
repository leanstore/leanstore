#pragma once
#include "ProfilingTable.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
class ResultsTable : public ProfilingTable
{
  public:
   u64 total_seconds = 0;
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
