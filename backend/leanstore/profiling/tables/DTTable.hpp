#pragma once
#include "ProfilingTable.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
using namespace storage;
class DTTable : public ProfilingTable
{
  private:
   string dt_name;
   u64 dt_id;
   BufferManager& bm;

  public:
   DTTable(BufferManager& bm);
   // -------------------------------------------------------------------------------------
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
