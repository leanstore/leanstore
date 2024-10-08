#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <cassert>
#include <chrono>
#include <thread>

/**
 * @brief Require a FIFO file to be created beforehand,
 *  which is passed to both perf record/stat and the benchmark program
 * Use perf_ctrl.sh script to activate the Perf Controller wrapper
 */
struct PerfController {
  int perf_ctrl_fd{-1};
  int ack_fd{-1};
  char ack_msg[5];

  // NOLINTBEGIN
  PerfController() {
    if (getenv("PERF_CTRL_FIFO")) {
      perf_ctrl_fd = open(getenv("PERF_CTRL_FIFO"), O_WRONLY);
      if (perf_ctrl_fd >= 0) {
        assert(getenv("PERF_CTRL_ACK"));
        ack_fd = open(getenv("PERF_CTRL_ACK"), O_RDONLY);
        assert(ack_fd >= 0);
        [[maybe_unused]] auto ret = write(perf_ctrl_fd, "disable\n", 9);
        assert(ret >= 0);
        ret = read(ack_fd, ack_msg, 5);
        assert(ret == 5 && strcmp(ack_msg, "ack\n") == 0);
      }
    }
  }

  ~PerfController() {
    StopPerfRuntime();
  }

  void StartPerfRuntime() {
    if (perf_ctrl_fd >= 0) {
      [[maybe_unused]] auto ret = write(perf_ctrl_fd, "enable\n", 8);
      assert(ret >= 0);
    }
  }

  void StopPerfRuntime() {
    if (perf_ctrl_fd >= 0) {
      [[maybe_unused]] auto ret = write(perf_ctrl_fd, "disable\n", 9);
      assert(ret >= 0);
      ret = read(ack_fd, ack_msg, 5);
      assert(ret == 5 && strcmp(ack_msg, "ack\n") == 0);
    }
  }
  // NOLINTEND
};
