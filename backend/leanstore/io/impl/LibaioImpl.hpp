#pragma once
// -------------------------------------------------------------------------------------
#include "../IoAbstraction.hpp"
#include "../Raid.hpp"
#include "../RequestStack.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <libaio.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <cstddef>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
// Common Linux code / shared by liabio and liburing
// -------------------------------------------------------------------------------------
class LinuxBaseChannel;
class LinuxBaseEnv {
protected:
   std::unique_ptr<RaidController<int>> raidCtl;
   IoOptions ioOptions;
public:
   ~LinuxBaseEnv() ;
   void init(IoOptions options) ;
   int deviceCount();
   int channelCount();
   u64 storageSize();
   // -------------------------------------------------------------------------------------
   RaidController<int>& getRaidCtl();
   // -------------------------------------------------------------------------------------
   virtual void* allocIoMemory(size_t size, size_t align);
   void* allocIoMemoryChecked(size_t size, size_t align);
   virtual void freeIoMemory(void* ptr, size_t size = 0);
   DeviceInformation getDeviceInfo();
};
class LinuxBaseChannel {
protected:
   RaidController<int>& raidCtl;
   IoOptions ioOptions;
public:
   LinuxBaseChannel(RaidController<int>& raidCtl, IoOptions ioOptions);
   void pushBlocking(IoRequestType type, char* data, s64 addr, u64 len, bool write_back);
   // -------------------------------------------------------------------------------------
   void printCounters(std::ostream& ss);
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
// Libaio
// -------------------------------------------------------------------------------------
class LibaioChannel;
class LibaioEnv : public LinuxBaseEnv
{
  public:
   std::unordered_map<int, std::unique_ptr<LibaioChannel>> channels;
   // -------------------------------------------------------------------------------------
   LibaioChannel& getIoChannel(int channel);
};
// -------------------------------------------------------------------------------------
struct LibaioIoRequest {
   struct iocb aio_iocb; // must be first
};
static_assert(offsetof(LibaioIoRequest, aio_iocb) == 0, "Required so pointers can be used as iocb pointers");
static_assert(offsetof(RaidRequest<LibaioIoRequest>, impl) == 0, "Required so pointers can be used as iocb pointers");
class LibaioChannel : public LinuxBaseChannel
{
   io_context_t aio_context;
   std::vector<iocb*> request_stack;
   int outstanding = 0;
   std::unique_ptr<struct io_event[]> events;
  public:
   LibaioChannel(RaidController<int>& raidCtl, IoOptions ioOptions);
   ~LibaioChannel();
   // -------------------------------------------------------------------------------------
   void _push(RaidRequest<LibaioIoRequest>* req) ;
   int _submit() ;
   int _poll(int min = 0) ;
   void _printSpecializedCounters(std::ostream& ss) ;
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
