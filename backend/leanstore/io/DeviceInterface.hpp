#if false
#pragma once
// -------------------------------------------------------------------------------------
#include "leanstore/io/IoOptions.hpp"
// -------------------------------------------------------------------------------------
namespace mean {
// -------------------------------------------------------------------------------------
class DeviceEnvironmentInterface 
{
  public:
   virtual ~DeviceEnvironmentInterface() { };
   virtual DeviceEnvironmentInterface& getIoChannel(int channel);
   virtual void init(IoOptions options) = 0;
   virtual void* allocIoMemory(size_t size, size_t align) = 0;
   virtual void* allocIoMemoryChecked(size_t size, size_t align) = 0;
   virtual void freeIoMemory(void* ptr, size_t size) = 0;
   virtual int deviceCount() = 0;
   virtual int channelCount() = 0;
   virtual u64 storageSize() = 0;
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
#endif
