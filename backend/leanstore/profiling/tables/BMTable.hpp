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
class BMTable : public ProfilingTable
{
  private:
   BufferManager& bm;
   s64 local_phase_1_ms = 0, local_phase_2_ms = 0, local_phase_3_ms = 0, local_poll_ms = 0, total;
   u64 local_total_free, local_total_cool;

  public:
   BMTable(BufferManager& bm);
   // -------------------------------------------------------------------------------------
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
