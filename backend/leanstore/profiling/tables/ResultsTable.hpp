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
  private:
    u64 total_seconds;
  public:
    virtual std::string getName();
    virtual void open();
    virtual void next();
    void setSeconds(u64 seconds) {
      total_seconds = seconds;
    }
};
}  // namespace profiling
}  // namespace leanstore
