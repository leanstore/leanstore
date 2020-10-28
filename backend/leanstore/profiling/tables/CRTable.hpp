#pragma once
#include "ProfilingTable.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
class CRTable : public ProfilingTable
{
  private:
  double p1, p2, total,write;

  public:
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
