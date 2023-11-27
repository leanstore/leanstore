#include "leanstore/config.h"

#include "gflags/gflags.h"
#include "gtest/gtest.h"

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_bm_aio_qd    = 8;
  FLAGS_worker_count = 1;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}