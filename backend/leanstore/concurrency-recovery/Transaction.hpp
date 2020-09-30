#pragma once
#include "Units.hpp"
#include "Exceptions.hpp"
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
  enum class STATE { IDLE, STARTED, COMMITED, ABORTED };
  STATE state = STATE::IDLE;
  u64 tx_id = 0;
  LID start_gsn = 0;
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
