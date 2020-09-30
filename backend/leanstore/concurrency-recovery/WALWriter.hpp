#pragma once
#include "Units.hpp"
#include "WAL.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
class WALWriter
{
  static s32 ssd_fd;
  static atomic<u64> ssd_offset;

 public:
  static void init(s32 ssd_fd, u64 ssd_offset);
  static void write(u8* src, u64 size);
};
}  // namespace cr
}  // namespace leanstore
