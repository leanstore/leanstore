#include "Units.hpp"
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  LeanStore leanstore;
  leanstore.startProfilingThread();
  // -------------------------------------------------------------------------------------
  while(true){

  }
  return 0;
}
