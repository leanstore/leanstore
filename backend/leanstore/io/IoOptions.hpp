#pragma once
// -------------------------------------------------------------------------------------
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <string>
#include <exception>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
struct IoOptions {
   std::string engine;
   std::string path;
   int iodepth = 256;
   int async_batch_submit = 1; // FIXME remove this, useless and makes e/ just complicatet. submit means submit.
   int async_batch_complete_max = 0;
   bool truncate = false;
   bool falloc = false;
   u64 write_back_buffer_size = 64 * 1024;
   // -------------------------------------------------------------------------------------
   bool ioUringPollMode = false;
   int ioUringShareWq = 0;
   bool ioUringNVMePassthrough = false;
   // -------------------------------------------------------------------------------------
   bool raid5 = false;
   int channelCount = 0;
   // -------------------------------------------------------------------------------------
   IoOptions() {}
   IoOptions(std::string engine, std::string path) : engine(engine), path(path) {}
   void check()
   {
      if (async_batch_submit > iodepth)
         throw std::logic_error("iodepth must be higher than async_batch_submit");
   }
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
