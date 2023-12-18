#pragma once
// -------------------------------------------------------------------------------------
#include "IoRequest.hpp"
#include "IoChannel.hpp"
#include "Time.hpp"
#include "Units.hpp"
#include "Exceptions.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/concurrency/ConnectedIoChannel.hpp"
#include "leanstore/utils/Hist.hpp"
#include "RequestStack.hpp"
#include "Raid.hpp"
#include "leanstore/profiling/counters/SSDCounters.hpp"
// -------------------------------------------------------------------------------------
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <cstring>
#include <vector>
#include <iomanip>
#include <fstream>
// -------------------------------------------------------------------------------------
namespace mean
{
struct DeviceInformation {
   struct Info {
      int id;
      std::string name;
      int fd = -1;
   };
   std::vector<Info> devices;
};
// -------------------------------------------------------------------------------------
template <typename TIoEnvironment, typename TIoChannel, typename TImplRequest>
class Raid0Channel : public IoChannel
{
   TIoEnvironment& io_env;
   TIoChannel& io_channel;
   IoOptions io_options;
   RequestStack<RaidRequest<TImplRequest>> request_stack;
   u64 pushTimeout = 0;
   int outstanding = 0;
   u64 pushed = 0;
   u64 pushedFromRemote = 0;
   u64 completed = 0;
   // ------------------------------------------------------------------------------------
//#define IO_TRACE_ON
#ifdef IO_TRACE_ON
   struct TraceElement {
      uint8_t type;
      uint32_t addr;
		uint32_t latency;
      uint64_t tsc;
      TraceElement(uint8_t _type, uint32_t _addr, uint64_t _tsc, uint32_t _latency) : type(_type), addr(_addr), latency(_latency), tsc(_tsc) {}
   };
   std::vector<TraceElement> trace;
#endif
   // -------------------------------------------------------------------------------------
   Raid0 raid;
   static const u64 CHUNK_SIZE = 64*1024;
   // -------------------------------------------------------------------------------------
   RemoteIoChannelClient remote_client;
   // -------------------------------------------------------------------------------------
  public:
   Raid0Channel(TIoEnvironment& io_env, TIoChannel& io_channel, IoOptions io_options, u64 channelId, u64 totalChannels) // TODO
      : IoChannel(io_env.deviceCount()), io_env(io_env), io_channel(io_channel), io_options(io_options), request_stack(io_options.iodepth), raid(io_env.deviceCount(), CHUNK_SIZE)
   {
#ifdef IO_TRACE_ON
      trace.reserve(100e6);
#endif
      for (int i = 0; i < request_stack.max_entries; i++) {
         request_stack.requests[i].base.write_back_buffer = (char*)io_env.allocIoMemoryChecked(io_options.write_back_buffer_size, 512);
      }
   };
   ~Raid0Channel() {
      static std::atomic<int> traceThread = 0;
      for (int i = 0; i < request_stack.max_entries; i++) {
          io_env.freeIoMemory(request_stack.requests[i].base.write_back_buffer, io_options.write_back_buffer_size);
      }
#ifdef IO_TRACE_ON
      const int thread = traceThread.fetch_add(1);
      std::cout << "write trace: " << trace.size() << std::endl;
      std::ofstream outFile("trace" + std::to_string(thread) +".csv");
      for (uint64_t i = 0; i < trace.size(); i++) {
         auto& t  = trace[i];
         outFile << i << "," << t.tsc << "," << (int)t.type << "," << t.addr << "," << t.latency<< std::endl;
      }
      outFile.close();
#endif
   };
   // -------------------------------------------------------------------------------------
   IoBaseRequest* getIoRequest() override { 
      RaidRequest<TImplRequest>* req = nullptr;
      if (!request_stack.popFromFreeStack(req)) {
         return nullptr;
      }
      return &req->base;
   }
   // -------------------------------------------------------------------------------------
   void pushIoRequest(IoBaseRequest* base_req) override { 
      const std::size_t offset = offsetof(RaidRequest<TImplRequest>, base);
      char *raid_request_ptr_char = reinterpret_cast<char *>(base_req) - offset; // a bit of a hack
      RaidRequest<TImplRequest>* req = reinterpret_cast<RaidRequest<TImplRequest>*>(raid_request_ptr_char);
		req->base.stats.push_time = readTSC();
      if (req->base.type == IoRequestType::Write && req->base.write_back) {
         req->base.write_back = true;
         assert(req->base.len <= io_options.write_back_buffer_size);
         std::memcpy(req->base.write_back_buffer, req->base.data, req->base.len);
      }
      req->base.out_of_place_addr = req->base.addr;
      pushed++;
      request_stack.pushToSubmitStack(req);
   };
   // -------------------------------------------------------------------------------------
   void _push(const IoBaseRequest& usr) override { 
      IoBaseRequest* req = getIoRequest();
      if (!req) {
         throw std::logic_error("Cannot push more: free: " + std::to_string(request_stack.free) + " pushed: " + std::to_string(request_stack.pushed)  + " max: " + std::to_string(request_stack.max_entries));
      }
      ensure(req);
      req->copyFields(usr);
      pushIoRequest(req);
   }
   // -------------------------------------------------------------------------------------
   int submitable() override {
      if (remote_client.remote_count > 0) {
         for (int i = 0; i < remote_client.remote_count; i++) {
            if (!remote_client.remotes[i]->submit_ring.empty()) {
               return 1;
            }
         }
      }
      return request_stack.submitStackSize();
   };
   int _submit() override { 
      //  
      if (remote_client.remote_count > 0) {
         for (int i = 0; i < remote_client.remote_count; i++) {
            IoBaseRequest* req;
            while (request_stack.free > request_stack.max_entries*0.25 && remote_client.remotes[i]->submit_ring.try_pop(req)) {
               // from remote to local
               push(req->type, req->data, req->addr, req->len, req->innerCallback, req->write_back);
               pushedFromRemote++;
            }
         }
      }
      // look at all pushed requests, calculate raid, push them to below, submit
      RaidRequest<TImplRequest>* req;
      while (request_stack.popFromSubmitStack(req)) {
         int device;
         u64 raidedOffset;
         assert(req->base.len <= CHUNK_SIZE);
         raid.calc(req->base.addr, device, raidedOffset);
         req->base.device = device;
         req->base.offset = raidedOffset;
         req->base.innerCallback.user_data.val.ptr = req;
         req->base.innerCallback.user_data2.val.ptr = this;
         req->base.innerCallback.callback = [](IoBaseRequest* req) {
            auto rr = reinterpret_cast<RaidRequest<TImplRequest>*>(req->innerCallback.user_data.val.ptr);
            rr->base.user.callback(&rr->base);
            auto this_ptr = (Raid0Channel<TIoEnvironment, TIoChannel,TImplRequest>*)req->innerCallback.user_data2.val.ptr;
            auto ch = reinterpret_cast<Raid0Channel<TIoEnvironment, TIoChannel,TImplRequest>*>(this_ptr);
            /*COUNTERS_BLOCK()*/ { ch->counters.handleCompletedReq(*req); leanstore::SSDCounters::myCounters().polled[req->device]++; }
            rr->base.stats.completion_time = readTSC();
#ifdef IO_TRACE_ON
            this_ptr->trace.emplace_back((int)rr->base.type, rr->base.addr, nanoFromTsc(rr->base.stats.submit_time), tscDifferenceUs(readTSC(), rr->base.stats.submit_time));
#endif
            if (!rr->base.reuse_request) {
               ch->request_stack.returnToFreeList(rr);
            }
         };
         outstanding++;
         COUNTERS_BLOCK() {
            if (req->base.type == IoRequestType::Write) {
               counters.outstandingWrite++;
            } else if (req->base.type == IoRequestType::Read) {
               counters.outstandingRead++;
            }
         }
         COUNTERS_BLOCK() { leanstore::SSDCounters::myCounters().pushed[device]++; }
         COUNTERS_BLOCK() { counters.handleSubmitReq(req->base); }
			req->base.stats.submit_time = readTSC();
         io_channel._push(req);
         __builtin_prefetch(&req->impl,0,1);
      }
      return io_channel._submit();
   };
   int _poll(int min = 0) override { 
      int ret = io_channel._poll(min);
      outstanding -= ret;
      completed += ret;
      return ret;
   };
   void _printSpecializedCounters(std::ostream& ss) override { io_channel._printSpecializedCounters(ss); };
   bool hasFreeIoRequests() override {
      return !request_stack.full();
   };
   bool readStackFull() override {
      return request_stack.full();
   }
   bool writeStackFull() override {
      return request_stack.free < (request_stack.max_entries * 0.5);
   }
   void registerRemoteChannel(RemoteIoChannel* rem) override {
      remote_client.registerRemote(rem); 
   }
   void pushBlocking(IoRequestType type, char* data, s64 addr, u64 len, bool write_back = false) override {
      io_channel.pushBlocking(type, data, addr, len, write_back);
   }
};
// -------------------------------------------------------------------------------------
/*
template <typename TIoEnvironment, typename TIoChannel, typename TImplRequest>
class Raid5Channel : public IoChannel
{
   TIoEnvironment& io_env;
   TIoChannel& io_channel;
   IoOptions io_options;
   RequestStack<RaidRequest<TImplRequest>> write_request_stack;
   RequestStack<RaidRequest<TImplRequest>> read_request_stack;
   // 
   std::vector<u64> free_addrs;
   u64 next_chunk;
   u64 chunk_distance;
   //
   static const u64 CHUNK_SIZE = 64*1024;
   Raid0 raid;
   //
   RemoteIoChannelClient remote_client;
  public:
   Raid5Channel(TIoEnvironment& io_env, TIoChannel& io_channel, IoOptions io_options, u64 channelId, u64 totalChannels) 
         : IoChannel(io_env.deviceCount()), io_env(io_env), io_channel(io_channel), io_options(io_options), write_request_stack(io_options.iodepth), read_request_stack(io_options.iodepth),
            next_chunk(channelId), chunk_distance(totalChannels), raid(io_env.deviceCount(), CHUNK_SIZE)
   {
      for (int i = 0; i < write_request_stack.max_entries; i++) {
         write_request_stack.requests[i].base.write_back_buffer = (char*)io_env.allocIoMemoryChecked(io_options.write_back_buffer_size, 512);
      }
      for (int i = 0; i < read_request_stack.max_entries; i++) {
         read_request_stack.requests[i].base.write_back_buffer = (char*)io_env.allocIoMemoryChecked(io_options.write_back_buffer_size, 512);
      }
   };
   ~Raid5Channel() {
      for (int i = 0; i < write_request_stack.max_entries; i++) {
          io_env.freeIoMemory(write_request_stack.requests[i].base.write_back_buffer, io_options.write_back_buffer_size);
      }
      for (int i = 0; i < read_request_stack.max_entries; i++) {
          io_env.freeIoMemory(read_request_stack.requests[i].base.write_back_buffer, io_options.write_back_buffer_size);
      }
   };
   // -------------------------------------------------------------------------------------
   void _push(const IoBaseRequest& usr) override { 
      RaidRequest<TImplRequest>* req = nullptr;
      switch (usr.type) {
         case IoRequestType::Write:
            if (!write_request_stack.pushToSubmitStack(req)) {
               throw std::logic_error("Cannot push more");
            }
            break;
         case IoRequestType::Read:
            if (!read_request_stack.pushToSubmitStack(req)) {
               throw std::logic_error("Cannot push more");
            }
            break;
         default:
            throw std::logic_error("not implemented");
      }
      ensure(req);
		req->base.stats.push_time = readTSC();
      if (usr.type == IoRequestType::Write && usr.write_back) {
         req->base.write_back = true;
         assert(usr.len <= io_options.write_back_buffer_size);
         std::memcpy(req->base.write_back_buffer, usr.data, usr.len);
      }
      req->base.copyFields(usr);
   };
   int submitable() override {
      return write_request_stack.submitStackSize() + read_request_stack.submitStackSize();
   };
   int _submit() override { 
      // READ
      RaidRequest<TImplRequest>* req;
      while (read_request_stack.popFromSubmitStack(req)) {
         int device;
         u64 raidedOffset;
         assert(req->base.len < CHUNK_SIZE);
         raid.calc(req->base.addr, device, raidedOffset);
         req->base.device = device;
         req->base.offset = raidedOffset;
         req->base.innerCallback.user_data.val.ptr = req;
         req->base.innerCallback.user_data2.val.ptr = this;
         req->base.innerCallback.callback = [](IoBaseRequest* req) {
            auto rr = reinterpret_cast<RaidRequest<TImplRequest>*>(req->innerCallback.user_data.val.ptr);
            rr->base.user.callback(&rr->base);
            auto ch = reinterpret_cast<Raid5Channel<TIoEnvironment, TIoChannel,TImplRequest>*>(req->innerCallback.user_data2.val.ptr);
            COUNTERS_BLOCK() { ch->counters.handleCompletedReq(*req); }
            ch->read_request_stack.returnToFreeList(rr);
         };
         COUNTERS_BLOCK() { counters.handleSubmitReq(req->base); }
         io_channel._push(req);
      }
      // WRITE
      // look at all pushed requests,      RaidRequest<TImplRequest>* req;
      int deviceCnt = io_env.deviceCount();
      //ensure(write_request_stack.submitStackSize() % (deviceCnt - 1) == 0);
      while (write_request_stack.submitStackSize() >= (deviceCnt - 1)) { // more requests than required for raid (minus parity)
         std::vector<RaidRequest<TImplRequest>*> stripe = std::vector<RaidRequest<TImplRequest>*>(deviceCnt);
         for (int i = 0; i < deviceCnt; i++) {
            write_request_stack.popFromSubmitStack(stripe[i]);
         }
         // calculate raid, push them to below, submit
         u64 raidedOffset;
         int device;
         // user io
         for (int i = 0; i < deviceCnt - 1; i++) { 
            // TODO do actual RAID
            RaidRequest<TImplRequest>* req = stripe[i];
            assert(req->base.len < CHUNK_SIZE);
            raid.calc(req->base.addr, device, raidedOffset);
            req->base.device = device;
            req->base.offset = raidedOffset;
            req->base.out_of_place_addr = req->base.addr;

            req->base.innerCallback.user_data.val.ptr = req;
            req->base.innerCallback.user_data2.val.ptr= this;
            req->base.innerCallback.callback = [](IoBaseRequest* req) {
               auto rr = reinterpret_cast<RaidRequest<TImplRequest>*>(req->innerCallback.user_data.val.ptr);
               rr->base.user.callback(&rr->base);
               auto ch = reinterpret_cast<Raid5Channel<TIoEnvironment, TIoChannel,TImplRequest>*>(req->innerCallback.user_data2.val.ptr);
               COUNTERS_BLOCK() { ch->counters.handleCompletedReq(*req); }
               ch->write_request_stack.returnToFreeList(rr);
            };
            COUNTERS_BLOCK() { counters.handleSubmitReq(req->base); }
            io_channel._push(req);
         }
      }
      return io_channel._submit();
   };
   int _poll(int min = 0) override { 
      return  io_channel._poll(min);
   };
   void _printSpecializedCounters(std::ostream& ss) override {io_channel._printSpecializedCounters(ss); };
   int submitMin() override {
      return io_env.deviceCount() - 1;
   };
   bool readStackFull() override {
      return read_request_stack.full();
   }
   bool writeStackFull() override {
      return write_request_stack.full();
   }
   void registerRemoteChannel(RemoteIoChannel* rem) override {
      remote_client.registerRemote(rem); 
   }
};
*/
// -------------------------------------------------------------------------------------
class RaidEnvironment
{
  public:
   virtual ~RaidEnvironment() {};
   // -------------------------------------------------------------------------------------
   virtual IoChannel& getIoChannel(int channel) = 0;
   virtual int channelCount() = 0;
   // -------------------------------------------------------------------------------------
   virtual u64 storageSize() = 0;
   // -------------------------------------------------------------------------------------
   virtual void freeIoMemory(void* ptr, size_t size = 0) = 0;
   void* allocIoMemoryChecked(size_t size, size_t align);
   // -------------------------------------------------------------------------------------
   virtual DeviceInformation getDeviceInfo() = 0;
  protected:
   virtual void* allocIoMemory(size_t size, size_t align) = 0;
};
template <typename TIoEnvironment, typename TIoChannel, typename TImplRequest>
class RaidEnv : public RaidEnvironment {
   std::unique_ptr<TIoEnvironment> io_env;
   std::vector<std::unique_ptr<IoChannel>> channels;
   IoOptions io_options;
  protected:
   void* allocIoMemory(size_t size, size_t align) override {
      return io_env->allocIoMemory(size, align);
   };
  public:
   RaidEnv(IoOptions options) : io_options(options) {
      io_env = std::unique_ptr<TIoEnvironment>(new TIoEnvironment());
      io_env->init(options);
      int io_env_max_channels = io_env->channelCount();
      int channelCount = options.channelCount > 0 ? options.channelCount : io_env_max_channels;
      channels.resize(channelCount);
      std::cout << "used channels: " << channels.size() << std::endl;
      for (int i = 0; i < channelCount; i++) {
         auto& ch = channels[i];
         if (i < io_env_max_channels) {
            if (io_options.raid5) {
               throw std::logic_error("not implemented");
               //ch = std::unique_ptr<IoChannel>(new Raid5Channel<TIoEnvironment, TIoChannel, TImplRequest>(*io_env, io_env->getIoChannel(i), io_options, i, io_options.channelCount));
            } else {
               ch = std::unique_ptr<IoChannel>(new Raid0Channel<TIoEnvironment, TIoChannel, TImplRequest>(*io_env, io_env->getIoChannel(i), io_options, i, io_options.channelCount));
            }
         } else {
            int remoteId = i % (io_env_max_channels);
            if (true) {
               remoteId = i - 64;// i % (io_env_max_channels);
               if (i == 63) {
                  remoteId = i - 1;
               } else if (i >= 126) {
                  remoteId = 63 - (i - 126 + 1);
               }
            }
            assert(remoteId < io_env_max_channels && remoteId >= 0);
            RemoteIoChannel* rem = new RemoteIoChannel(io_options);
            ch = std::unique_ptr<RemoteIoChannel>(rem);
            channels[remoteId]->registerRemoteChannel(rem);
            std::cout << "i: " << i << " remoteId: " << remoteId << std::endl;
         }
      }
   };
   ~RaidEnv() {};
   // -------------------------------------------------------------------------------------
   IoChannel& getIoChannel(int channel) override {
      return *channels.at(channel);
   };
   int channelCount() override {
      return 1000000;// io_env->channelCount();
   };
   // -------------------------------------------------------------------------------------
   void freeIoMemory(void* ptr, size_t size = 0) override {
      io_env->freeIoMemory(ptr, size);
   };
   // -------------------------------------------------------------------------------------
   u64 storageSize() override {
      // TODO raided size...
      return io_env->storageSize();
   };
   DeviceInformation getDeviceInfo() override {
      return io_env->getDeviceInfo();
   };
};
/*
class IoEnvironment
{
  protected:
   virtual void* allocIoMemory(size_t size, size_t align);
  public:
   virtual ~IoEnvironment(){};
   virtual void init(IoOptions options) = 0;
   // -------------------------------------------------------------------------------------
   virtual DeviceAccessChannel& getIoChannel(int channel) = 0;
   virtual int channelCount() = 0;
   // -------------------------------------------------------------------------------------
   virtual void* allocIoMemory(size_t size, size_t align);
   void* allocIoMemoryChecked(size_t size, size_t align);
   virtual void freeIoMemory(void* ptr, size_t size = 0);
   // -------------------------------------------------------------------------------------
   virtual u64 storageSize() = 0;
};
*/
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
