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
   u64 wal_hits, wal_miss;
   double p1, p2, total, write, wal_total, wal_hit_pct, wal_miss_pct;

   u64 local_tx;
   u64 local_tx_lat95p_us = 0;
   u64 local_tx_lat99p_us = 0;
   u64 local_tx_lat99p9_us = 0;

  public:
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
