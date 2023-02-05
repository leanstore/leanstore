#pragma once
#if LEANSTORE_INCLUDE_XNVME
// -------------------------------------------------------------------------------------
#include "../IoAbstraction.hpp"
#include "../RequestStack.hpp"
#include "../Raid.hpp"
// -------------------------------------------------------------------------------------
#include "libxnvme.h"
#include "libxnvme_pp.h"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <memory>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
/*
struct SpdkRequest : SpdkIoReq {
   IoBaseRequest base;
   std::mutex mutex;
   std::condition_variable cond_var;
   std::atomic<bool> done;
};
*/
// -------------------------------------------------------------------------------------
class XnvmeChannel;
struct XnvmeRequest {
};
struct XnvmeDevQueues {
   xnvme_dev* dev;
   int nsid;
   std::vector<xnvme_queue*> queues;
};
class XnvmeEnv 
{
   std::unique_ptr<RaidController<XnvmeDevQueues>> raidCtl;
   IoOptions ioOptions;
   std::vector<std::unique_ptr<XnvmeChannel>> channels;

  public:
   ~XnvmeEnv();
   void init(IoOptions options);
   XnvmeChannel& getIoChannel(int channel);
   void* allocIoMemory(size_t size, size_t align);
   void* allocIoMemoryChecked(size_t size, size_t align);
   void freeIoMemory(void* ptr, size_t size);
   int deviceCount();
   int channelCount();
   u64 storageSize();
   DeviceInformation getDeviceInfo();
};
// -------------------------------------------------------------------------------------
class XnvmeChannel
{
   std::vector<RaidRequest<XnvmeRequest>*> write_request_stack;

   IoOptions ioOptions;
   XnvmeEnv& env;
   RaidController<XnvmeDevQueues>& raidCtl;
   int queue;

   void prepare_request(RaidRequest<XnvmeRequest>* req);
   void completion(IoBaseRequest* req);
   // -------------------------------------------------------------------------------------
  public:
   XnvmeChannel(IoOptions options, RaidController<XnvmeDevQueues>& raidCtl, int queue, XnvmeEnv& env);
   ~XnvmeChannel() ;
   // -------------------------------------------------------------------------------------
   template<typename SubmissionFun>
   void submission(SubmissionFun subFun, RaidRequest<XnvmeRequest>* req);
   // -------------------------------------------------------------------------------------
   void _push(RaidRequest<XnvmeRequest>* req) ;
   void pushBlocking(IoRequestType type, char* data, s64 addr, u64 len, bool write_back) { throw std::logic_error("not implemented"); }
   int _submit() ;
   int _poll(int min = 0) ;
   void _printSpecializedCounters(std::ostream& ss) ;
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
#endif // LEANSTORE_INCLUDE_XNVME
