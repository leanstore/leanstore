#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/BTreeSlotted.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
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
  // JumpMU Exps
  struct X {
    X() { jumpmu_registerDestructor(); }
    jumpmu_defineCustomDestructor(X) ~X()
    {
      cout << "~X()" << endl;
      jumpmu::clearLastDestructor();
    }
  };
  struct Y {
    struct X x;
    Y() { jumpmu_registerDestructor(); }
    jumpmu_defineCustomDestructor(Y) ~Y()
    {
      cout << "~Y()" << endl;
      jumpmu::clearLastDestructor();
    }
  };
  return 0;
  jumpmuTry()
  {
    struct Y y;
    jumpmu::jump();
  }
  jumpmuCatch() {}
  // -------------------------------------------------------------------------------------
  return 0;
}
