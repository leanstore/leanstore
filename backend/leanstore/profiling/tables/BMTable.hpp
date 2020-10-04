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
using namespace buffermanager;
class BMTable : public ProfilingTable
{
 private:
  BufferManager& bm;

 public:
  BMTable(BufferManager& bm);
  // -------------------------------------------------------------------------------------
  virtual std::string getName();
  virtual void open();
  virtual void next();
  virtual std::vector<Column>& getColumns();
};
}  // namespace profiling
}  // namespace leanstore
