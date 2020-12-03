#pragma once
#include "Units.hpp"
#include "Worker.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct WALEntry {
   enum class TYPE : u8 { TX_START, TX_COMMIT, TX_ABORT, DT_SPECIFIC, CARRIAGE_RETURN };
   // -------------------------------------------------------------------------------------
   u64 magic_debugging_number = 99;
   std::atomic<LID> lsn;
   u16 size;
   TYPE type;
   void computeCRC() { magic_debugging_number = utils::CRC(reinterpret_cast<u8*>(this) + sizeof(u64), size - sizeof(u64)); }
   void checkCRC() const
   {
      if (magic_debugging_number != utils::CRC(reinterpret_cast<const u8*>(this) + sizeof(u64), size - sizeof(u64))) {
         raise(SIGTRAP);
         ensure(false);
      }
   }
};
// -------------------------------------------------------------------------------------
struct WALMetaEntry : WALEntry {
};
// static_assert(sizeof(WALMetaEntry) == 32, "");
// -------------------------------------------------------------------------------------
struct WALDTEntry : WALEntry {
   LID gsn;
   DTID dt_id;
   PID pid;
   u8 payload[];
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
