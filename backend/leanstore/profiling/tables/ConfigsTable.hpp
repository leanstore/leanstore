#pragma once
#include "ProfilingTable.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
class ConfigsTable : public ProfilingTable
{
  public:
   virtual std::string getName();
   virtual void open();
   virtual void next();
   u64 hash();
   void add(string name, string value);
};
}  // namespace profiling
}  // namespace leanstore
