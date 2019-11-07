#include "LeanStore.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DEFINE_bool(log_stdout, false, "");
// -------------------------------------------------------------------------------------
namespace leanstore {
LeanStore::LeanStore()
{
   // Set the default logger to file logger
   if ( !FLAGS_log_stdout ) {
      auto file_logger = spdlog::rotating_logger_mt("main_logger", "log.txt",
                                                    1024 * 1024 * 10, 3);
      spdlog::set_default_logger(file_logger);
   }
   BMC::start();
}
}
// -------------------------------------------------------------------------------------
