#pragma once

#include <unordered_map>
#include "IoAbstraction.hpp"
#include "leanstore/concurrency/MessageHandler.hpp"
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
/*
class RaidChannelManger
{
  public:
   std::unique_ptr<IoEnvironment> io_env  = nullptr;
   std::unordered_map<int, std::unique_ptr<IoChannel>> channels;
   IoOptions ioOptions;
   // -------------------------------------------------------------------------------------
   RaidChannelManger(IoOptions options);
   IoChannel& getIoChannel(int channel);
   int channelCount();
   u64 storageSize(); 
};
*/
class IoInterface
{
   static std::unique_ptr<RaidEnvironment> _instance;
   IoInterface() = delete;
   IoInterface(const IoInterface&) = delete;
  public:
   static RaidEnvironment& initInstance(IoOptions ioOptions); // TODO pretty ugly to have mm here
   static RaidEnvironment& instance();
   // -------------------------------------------------------------------------------------
   static int channelCount();
   static IoChannel& getIoChannel(int channel);
   static void* allocIoMemoryChecked(size_t size, size_t align);
   static void freeIoMemory(void* ptr, size_t size = 0);
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
