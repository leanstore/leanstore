#include <gflags/gflags.h>

#include "Exceptions.hpp"
#include "PerfEvent.hpp"
#include "Units.hpp"
#include "leanstore/utils/FVector.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include <fstream>
// -------------------------------------------------------------------------------------
DEFINE_string(in, "", "");
DEFINE_string(out, "", "");
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  vector<string> strings;
  std::ifstream input_file(FLAGS_in);
  std::string line;
  while (getline(input_file, line)) {
    strings.push_back(line);
  }
  // -------------------------------------------------------------------------------------
  cout << "read # = " << strings.size() << endl;
  leanstore::utils::writeBinary(FLAGS_out.c_str(), strings);
  return 0;
}
