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
   u64 local_tx_lat10p_us = 0;
   u64 local_tx_lat25p_us = 0;
   u64 local_tx_lat50p_us = 0;
   u64 local_tx_lat95p_us = 0;
   u64 local_tx_lat99p_us = 0;
   u64 local_tx_lat99p9_us = 0;
   u64 local_tx_lat99p99_us = 0;

   u64 local_tx_lat10pi_us = 0;
   u64 local_tx_lat25pi_us = 0;
   u64 local_tx_lat50pi_us = 0;
   u64 local_tx_lat95pi_us = 0;
   u64 local_tx_lat99pi_us = 0;
   u64 local_tx_lat99pi9_us = 0;
   u64 local_tx_lat99pi99_us = 0;

   u64 local_ssd_read_lat50p_us = 0;
   u64 local_ssd_read_lat99p_us = 0;
   u64 local_ssd_read_lat99p9_us = 0;
   u64 local_ssd_read_lat99p99_us = 0;

   u64 local_ssd_write_lat50p_us = 0;
   u64 local_ssd_write_lat99p_us = 0;
   u64 local_ssd_write_lat99p9_us = 0;
   u64 local_ssd_write_lat99p99_us = 0;

  public:
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
