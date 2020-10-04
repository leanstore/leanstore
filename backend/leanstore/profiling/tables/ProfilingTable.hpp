#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <sstream>
#include <string>
#include <vector>
#include <iomanip>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
using ColumnGenerator = std::function<void(std::vector<std::string>& values)>;
using ColumnValues = std::vector<std::string>;
struct Column {
  std::string name;
  ColumnGenerator generator;
  std::vector<std::string> values;
  Column(std::string name, ColumnGenerator&& g) : name(name), generator(g) {}
};
class ProfilingTable
{
 protected:
  std::vector<Column> columns;
  // -------------------------------------------------------------------------------------
  std::string to_string(s8 x) { return std::to_string(x); }
  std::string to_string(s16 x) { return std::to_string(x); }
  std::string to_string(s32 x) { return std::to_string(x); }
  std::string to_string(s64 x) { return std::to_string(x); }
  std::string to_string(u8 x) { return std::to_string(x); }
  std::string to_string(u16 x) { return std::to_string(x); }
  std::string to_string(u32 x) { return std::to_string(x); }
  std::string to_string(u64 x) { return std::to_string(x); }
  std::string to_string(double x)
  {
    std::stringstream stream;
    stream << std::fixed << std::setprecision(1) << x;
    return stream.str();
  }
  std::string to_string(float x)
  {
    std::stringstream stream;
    stream << std::fixed << std::setprecision(1) << x;
    return stream.str();
  }
  std::string to_string(std::string x) { return x; }

 public:
  // Open -> getColumns() -> next -> getColumns()
  virtual std::string getName() = 0;
  virtual void open() = 0;
  virtual void next() = 0;
  virtual std::vector<Column>& getColumns() { return columns; }
};
}  // namespace profiling
}  // namespace leanstore
