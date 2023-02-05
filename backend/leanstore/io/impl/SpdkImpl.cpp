#include "SpdkImpl.hpp"
// -------------------------------------------------------------------------------------
#include <cstring>
#include "leanstore/io/IoChannel.hpp"

namespace mean
{
// -------------------------------------------------------------------------------------
SpdkEnv::~SpdkEnv()
{
   controller.reset(nullptr); 
   std::cout << "SpdkEnv deinit" << std::endl;
   SpdkEnvironment::deinit();
}
// -------------------------------------------------------------------------------------
void SpdkEnv::init(IoOptions options)
{
   std::thread([] {  // hack so that the spdk pinning has no influence on leanstore threads
      SpdkEnvironment::init();
   }).join();
   controller = std::make_unique<NVMeMultiController>();
   // controller = std::make_unique<NVMeRaid0>();
   controller->connect(options.path);
   controller->allocateQPairs();
   // TODO allocate maximum number of IoChannels
   // make them returnable by below function
   // use controller qpairSize()
   int qs = controller->qpairSize();
   for (int i = 0; i < qs; i++) {
      channels.push_back(std::make_unique<SpdkChannel>(options, *controller, i));
   }
}

SpdkChannel& SpdkEnv::getIoChannel(int channel)
{
   ensure(channels.size() > 0, "don't forget to initizalize the io env first");
   ensure(channel < (int)channels.size(), "There are only " + std::to_string(channels.size()) + " channels available.");
   // std::cout << "getChannel: " << channel << std::endl << std::flush;
   return *channels.at(channel);
}

int SpdkEnv::deviceCount()
{
   return controller->deviceCount();
}

int SpdkEnv::channelCount()
{
   return controller->qpairSize();
}

u64 SpdkEnv::storageSize()
{
   return controller->nsSize();
}

void* SpdkEnv::allocIoMemory(size_t size, size_t align)
{
   return SpdkEnvironment::dma_malloc(size, align);
}

void* SpdkEnv::allocIoMemoryChecked(size_t size, size_t align)
{
   auto mem = SpdkEnvironment::dma_malloc(size, align);
   null_check(mem, "Memory allocation failed");
   return mem;
}
void SpdkEnv::freeIoMemory(void* ptr, [[maybe_unused]]size_t size)
{
   SpdkEnvironment::dma_free(ptr);
}

DeviceInformation SpdkEnv::getDeviceInfo() {
   DeviceInformation d;
   d.devices.resize(deviceCount());
   for (unsigned int i = 0; i < controller->controller.size(); i++) {
      d.devices[i].id = i;
      d.devices[i].name = controller->controller[i].pciefile;
   }
   return d;
}
// -------------------------------------------------------------------------------------
// Channel 
// -------------------------------------------------------------------------------------
SpdkChannel::SpdkChannel(IoOptions ioOptions, NVMeMultiController& controller, int queue) 
   : options(ioOptions), controller(controller), queue(queue), lbaSize(controller.nsLbaDataSize())
{
   write_request_stack.reserve(ioOptions.iodepth);
   int c = controller.deviceCount();
   for (int i = 0; i < c; i++) {
      qpairs.emplace_back(controller.controller[i].qpairs[queue]);
      nameSpaces.emplace_back(controller.controller[i].nameSpace);
   }
}

SpdkChannel::~SpdkChannel()
{
}

void SpdkChannel::prepare_request(RaidRequest<SpdkIoReq>* req, SpdkIoReqCallback spdkCb)
{
   // base
   req->base.stats.push();
   switch (req->base.type) {
      case IoRequestType::Read:
         req->impl.type = SpdkIoReqType::Read;
         break;
      case IoRequestType::Write:
         req->impl.type = SpdkIoReqType::Write;
         break;
      default:
         throw std::logic_error("IoRequestType not supported" + std::to_string((int)req->base.type));
   }
   //
   req->impl.buf = req->base.buffer();
   req->impl.lba = req->base.offset / lbaSize;
   req->impl.lba_count = req->base.len / lbaSize;
   req->impl.callback = spdkCb;
}

void SpdkChannel::_push(RaidRequest<SpdkIoReq>* req)
{
   req->impl.this_ptr = this;
   prepare_request(req, [](SpdkIoReq* io_req) {
         auto req = reinterpret_cast<RaidRequest<SpdkIoReq>*>(io_req);
         req->base.innerCallback.callback(&req->base);
   });
   write_request_stack.push_back(req);
}

void SpdkChannel::_printSpecializedCounters(std::ostream& ss)
{
   ss << "spdk: ";
}

/*
//static thread_local ThreadNotifier::Message* notifier_request;
u64 InterfaceBase::_readSync(char* destination, u64 len, u64 addr) override {
        // one of the following three flags must be defined
#define IO_SPDK_SYNC_CV
        //#define IO_SPDK_SYNC_POLL
        //#define IO_SPDK_SYNC_NOTIFIER_POLL

#ifdef IO_SPDK_SYNC_NOTIFIER_POLL
        if (notifier_request == nullptr) {
                notifier_request = thread_notifier.registerThread();
        }
#endif
        SpdkIoReq req;
        prepare_request(&req, destination, addr, len, 0, 0, IoRequestType::Read, [&](SpdkIoReq* io_req){
                        auto req	= reinterpret_cast<SpdkIoReq*>(io_req);
#ifdef IOCOUNTERS
                        req->base.completion_time = std::chrono::high_resolution_clock::now();
#endif
#ifdef IO_SPDK_SYNC_CV
                        std::unique_lock<std::mutex> lock(req->mutex);
#endif
                        req->done = true;
#ifdef IO_SPDK_SYNC_CV
                        req->cond_var.notify_one();
#endif
                        }, nullptr);

#ifdef IO_SPDK_SYNC_NOTIFIER_POLL
        notifier_request->user_data[0] = reinterpret_cast<u64>(&req);
        notifier_request->toggle_bit = 1;
#endif
#ifndef IO_SPDK_SYNC_POLL
        std::unique_lock<std::mutex> lock(req.mutex);
        req.cond_var.wait(lock, [&] { return req.done.load(); });
#else
        while (!req.done.load()) {}
#endif

#ifdef IOCOUNTERS
        auto& cnters = WorkerCounters::myCounters();
        cnters.read_push_submit_latency += std::chrono::duration_cast<std::chrono::nanoseconds>(req.base.submit_time - req.base.push_time).count();
        cnters.read_push_completion_latency += std::chrono::duration_cast<std::chrono::nanoseconds>(req.base.completion_time -
req.base.push_time).count(); auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() -
req.base.push_time).count(); cnters.read_push_done_latency +=	diff; cnters.read_push_done_hist.increaseSlot(diff/1000); cnters.reads++; #endif
        return len;
}

int InterfaceBase::_syncReadsPollServer() override {
        auto message = thread_notifier.poll();
        LEANSTORE_BLOCK( PPCounters::myCounters().poll_cycles++; );
        if (message != nullptr) {
                auto req = reinterpret_cast<SpdkIoReq*>(message->user_data[0]);
                req->base.submit_time = std::chrono::high_resolution_clock::now();
                controller.submit(1, reinterpret_cast<SpdkIoReq*>(message->user_data[0]));
                message->toggle_bit = 0;
        }
        controller.process(1, 0);
        return 0;
}
*/
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
