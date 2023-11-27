/*
Copyright (c) 2023 Duy Nguyen

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

#pragma once

#include "gflags/gflags.h"

#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <cassert>

DEFINE_string(perf_controller_fifo, "", "Path to the FIFO pipe used to send signals to perf record/stat");

/**
 * @brief Require a FIFO file to be created beforehand,
 *  which is passed to both perf record/stat and the benchmark program
 * Use perf_ctrl.sh script to activate the Perf Controller wrapper
 *
 * e.g.
 * ./perf_ctrl.sh ./benchmark_cmd -a=1 -b=2
 */
struct PerfController {
  int perf_ctrl_fd{-1};

  PerfController() {
    if (FLAGS_perf_controller_fifo.size() > 0) {
      perf_ctrl_fd = open(FLAGS_perf_controller_fifo.c_str(), O_WRONLY);
      if (perf_ctrl_fd >= 0) {
        [[maybe_unused]] auto ret = write(perf_ctrl_fd, "disable\n", 9);
        assert(ret >= 0);
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
    }
  }
};
