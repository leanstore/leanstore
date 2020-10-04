#include "BMTable.hpp"

#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
BMTable::BMTable(BufferManager& bm) : ProfilingTable(), bm(bm) {}
// -------------------------------------------------------------------------------------
std::string BMTable::getName()
{
  return "bm";
}
// -------------------------------------------------------------------------------------
void BMTable::open()
{
  columns.emplace_back("space_usage_gib", [&](ColumnValues& values) {
    const double gib = bm.consumedPages() * 1.0 * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0;
    values.push_back(to_string(gib));
  });
}
// -------------------------------------------------------------------------------------
void BMTable::next()
{
  for (auto& c : columns) {
    c.generator(c.values);
  }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
