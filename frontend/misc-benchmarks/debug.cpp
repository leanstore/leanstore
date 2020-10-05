#include "Units.hpp"
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  {
    tabulate::Table table;
    table.add_row({"TX", "CPU", "Instructions"});
    table.add_row({"0", "1", "2"});
    table[1].format().hide_border_top();
    table[1].format().hide_border_bottom();
    cout << table << endl;
  }
  for (u64 t_i = 0; t_i < 4; t_i++) {
    tabulate::Table table;
    table.add_row({"TX", "CPU", "Instructions"});
    table.add_row({"0", "1", "2"});
    std::stringstream ss;
    table.print(ss);
    string str = ss.str();
    u8 count = 0;
    for (u64 i = 0; i < str.size(); i++) {
      if (str[i] == '\n') {
        count++;
      }
      if (count == 3) {
        cout << str[i];
      }
    }
  }
    cout << endl;
  return 0;
}
