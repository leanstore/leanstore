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
   u64 local_pp_submit_cnt, local_pp_submitted;
   u64 local_pp_qlen_cnt;
   u64 local_submitted = 0;
   u64 local_submit_calls = 0;

  public:
   BMTable(BufferManager& bm);
   // -------------------------------------------------------------------------------------
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
