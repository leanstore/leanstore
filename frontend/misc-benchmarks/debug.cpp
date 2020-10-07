#include "Units.hpp"
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  tabulate::Table table;
  table.add_row({"TX", "CPU", "Instructions", "sd"});
  table.add_row({"0", "1", "2", "3"});
  table.format().width(20);
  cout << table << endl;
  return 0;
}
