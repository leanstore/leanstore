#include <gflags/gflags.h>

#include "Exceptions.hpp"
#include "PerfEvent.hpp"
#include "Units.hpp"
#include "leanstore/utils/FVector.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_string(prog, "", "");
DEFINE_string(in, "", "");
DEFINE_string(out, "", "");
DEFINE_uint64(N, 10, "");
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  vector<string> strings;
  if (FLAGS_prog == "txt_to_vector") {
    std::ifstream input_file(FLAGS_in);
    std::string line;
    while (getline(input_file, line)) {
      strings.push_back(line);
    }
    // -------------------------------------------------------------------------------------
    cout << "read # = " << strings.size() << endl;
    leanstore::utils::writeBinary(FLAGS_out.c_str(), strings);
  } else if (FLAGS_prog == "sort") {
    leanstore::utils::FVector<std::string_view> input_strings(FLAGS_in.c_str());
    for (u64 i = 0; i < input_strings.size(); i++) {
      strings.push_back(std::string(input_strings[i]));
    }
    std::sort(strings.begin(), strings.end());
    leanstore::utils::writeBinary(FLAGS_out.c_str(), strings);
  } else if (FLAGS_prog == "read") {
    leanstore::utils::FVector<std::string_view> input_strings(FLAGS_in.c_str());
    for (u64 i = 0; i < FLAGS_N; i++) {
      cout << input_strings[i] << endl;
    }
    for (u64 i = input_strings.size() - 1; i > input_strings.size() - FLAGS_N; i--) {
      cout << input_strings[i] << endl;
    }
  }
  return 0;
}
