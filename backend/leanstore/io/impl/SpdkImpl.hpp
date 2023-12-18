#pragma once
#if LEANSTORE_INCLUDE_SPDK
// -------------------------------------------------------------------------------------
#include "Spdk.hpp"
// -------------------------------------------------------------------------------------
#include "../IoAbstraction.hpp"
#include "../RequestStack.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <vector>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
class SpdkChannel;
class SpdkEnv 
{
   std::unique_ptr<NVMeMultiController> controller;
   std::vector<std::unique_ptr<SpdkChannel>> channels;
  public:
   ~SpdkEnv();
   void init(IoOptions options);
   SpdkChannel& getIoChannel(int channel);
   void* allocIoMemory(size_t size, size_t align);
   void* allocIoMemoryChecked(size_t size, size_t align);
   void freeIoMemory(void* ptr, size_t size);
   int deviceCount();
   int channelCount();
   u64 storageSize();
   DeviceInformation getDeviceInfo();
};
// -------------------------------------------------------------------------------------
class SpdkChannel 
{
   std::vector<RaidRequest<SpdkIoReq>*> write_request_stack;

   IoOptions options;
   NVMeMultiController& controller;
   int queue;
   const int lbaSize;
   std::vector<spdk_nvme_qpair*> qpairs;
   std::vector<struct spdk_nvme_ns*> nameSpaces;
   std::vector<int> outstanding;
   // -------------------------------------------------------------------------------------
   void prepare_request(RaidRequest<SpdkIoReq>* req, SpdkIoReqCallback spdkCb);
   // -------------------------------------------------------------------------------------
  public:
   SpdkChannel(IoOptions options, NVMeMultiController& controller, int queue) ;
   ~SpdkChannel();
   // -------------------------------------------------------------------------------------
   void _push(RaidRequest<SpdkIoReq>* req);
   void pushBlocking(IoRequestType type, char* data, s64 addr, u64 len, bool write_back) { throw std::logic_error("not implemented"); }
   int _submit()
   {
      for (auto& req: write_request_stack) {
         int ret;
         if (true || outstanding[req->base.device] < 5) {
            ret = SpdkEnvironment::spdk_req_type_fun_lookup[(int)req->impl.type](nameSpaces[req->base.device], qpairs[req->base.device], req->impl.buf, req->impl.lba, req->impl.lba_count, NVMeController::completion, req, 0);
            outstanding[req->base.device]++;
            ensure(ret == 0);
            req = nullptr;
         }
         //controller.submit(req->base.device, queue, reinterpret_cast<SpdkIoReq*>(&req->impl));
      }
      int left = 0;
      for (int i = 0; i < write_request_stack.size(); i++) {
         if (write_request_stack[i] != nullptr) {
            write_request_stack[left++] = write_request_stack[i];
         }
      }
      int submitted = write_request_stack.size() - left;
      write_request_stack.resize(left);
      //write_request_stack.clear();
      return submitted;
   }

   int _poll(int)
   {
      int done = 0;
      /*
      bool keepPolling = false;
      do {
         for (unsigned int i = 0; i < qpairs.size(); i++) {
            int ok = spdk_nvme_qpair_process_completions(qpairs[i], 0);
            outstanding[i] -= ok;
            ensure(ok >= 0);
            done += ok;
         }
         keepPolling = false;
         for (auto o: outstanding) {
            if (o > 5)
               keepPolling = true;
         }
      } while (keepPolling);
      //done = controller.process(queue, 0);
      */
      for (unsigned int i = 0; i < qpairs.size(); i++) {
         int ok = spdk_nvme_qpair_process_completions(qpairs[i], 0);
         outstanding[i] -= ok;
         ensure(ok >= 0);
         done += ok;
      }
      assert(done >= 0);
      return done;
   }
   void _printSpecializedCounters(std::ostream& ss);
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
#endif
