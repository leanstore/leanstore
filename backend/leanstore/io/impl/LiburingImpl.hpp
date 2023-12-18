#pragma once
// -------------------------------------------------------------------------------------
#include "LibaioImpl.hpp"
#include "../IoAbstraction.hpp"
#include "../Raid.hpp"
#include "../RequestStack.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/io/IoRequest.hpp"
#include "liburing.h"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace mean
{
class LiburingChannel;
class LiburingEnv : public LinuxBaseEnv
{
  public:
   std::unordered_map<int, std::unique_ptr<LiburingChannel>> channels;
   // -------------------------------------------------------------------------------------
   LiburingChannel& getIoChannel(int channel);
};
// -------------------------------------------------------------------------------------
struct LiburingIoRequest {
   IoBaseRequest base;
   LiburingIoRequest() {}
   struct iovec iov;  // kind of a hack
};
class LiburingChannel : public LinuxBaseChannel
{
   struct io_uring ring;
   std::vector<RaidRequest<LiburingIoRequest>*> request_stack;
   int outstanding = 0;
   int nothingPolledStarving = 0;
   int lba_sz = -1;
public:
   LiburingChannel(RaidController<int>& raidCtl, IoOptions ioOptions, LiburingEnv& env);
   ~LiburingChannel();
   // -------------------------------------------------------------------------------------
   void _push(RaidRequest<LiburingIoRequest>* req) ;
   int _submit() ;
   int _poll(int min = 0) ;
   void _printSpecializedCounters(std::ostream& ss) ;
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
