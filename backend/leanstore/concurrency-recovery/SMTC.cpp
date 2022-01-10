#include "SMTC.hpp"

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
// To avoid race condition when loading a worker current start timestamp:
// The worker should set temporarily set its start ts to global ts before drawing a new one  [mmm, buggy]
//
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
