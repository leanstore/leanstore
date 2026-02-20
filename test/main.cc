#include "leanstore/config.h"

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count                = 1;
  FLAGS_txn_default_isolation_level = "ru";

  google::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::debug);
  return RUN_ALL_TESTS();
}