#include "WALWriter.hpp"

#include "CRMG.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <unistd.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
s32 WALWriter::ssd_fd = -1;
atomic<u64> WALWriter::ssd_offset;
// -------------------------------------------------------------------------------------
void WALWriter::init(s32 ssd_fd, u64 ssd_offset)
{
  WALWriter::ssd_fd = ssd_fd;
  WALWriter::ssd_offset = ssd_offset;
}
// -------------------------------------------------------------------------------------
void WALWriter::write(u8* src, u64 size)
{
  u64 offset = ssd_offset.fetch_add(-size) - size;
  s64 ret = pwrite(ssd_fd, src, size, offset);
  posix_check(ret != -1);
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
