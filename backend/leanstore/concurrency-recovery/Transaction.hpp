#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct Transaction {
   enum class TYPE : u8 { USER, SYSTEM };
   enum class STATE { IDLE, STARTED, READY_TO_COMMIT, COMMITED, ABORTED };
   STATE state = STATE::IDLE;
   u64 commit_mark = 0;
   LID min_observed_gsn_when_started, max_observed_gsn;
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
