#include "LibaioImpl.hpp"
// -------------------------------------------------------------------------------------
#include "../IoInterface.hpp"
#include "leanstore/io/IoOptions.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <cassert>
#include <libaio.h>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
// Linux Base Env
// -------------------------------------------------------------------------------------
LinuxBaseEnv::~LinuxBaseEnv() {
   raidCtl->forEach([]([[maybe_unused]]std::string& dev, int& fd) {
      close(fd);
      fd = -1;
   });
}
void LinuxBaseEnv::init(IoOptions ioOpts)
{
   this->ioOptions = ioOpts;
   raidCtl = std::make_unique<RaidController<int>>(ioOptions.path);
   raidCtl->forEach([this](std::string& dev, int& fd) {
      int flags = O_RDWR | O_DIRECT | O_NOATIME;
      if (this->ioOptions.truncate) {
         flags |= O_TRUNC | O_CREAT;
      }
      // -------------------------------------------------------------------------------------
      fd = open(dev.c_str(), flags, 0600);
      // -------------------------------------------------------------------------------------
      posix_check(fd > -1, "open file failed ");
      if (ioOptions.falloc > 0) {
         const u64 gib_size = 1024ull * 1024ull * 1024ull;
         auto dummy_data = (u8*)allocIoMemory(gib_size, 512);
         for (u64 i = 0; i < ioOptions.falloc; i++) {
            const int ret = pwrite(fd, dummy_data, gib_size, gib_size * i);
            posix_check(ret == gib_size);
         }
         freeIoMemory(dummy_data);
         fsync(fd);
      }
      ensure(fcntl(fd, F_GETFL) != -1);
      std::cout << "connected to: " << dev << std::endl;
   });
}
int LinuxBaseEnv::deviceCount()
{
   return raidCtl->deviceCount();;
}
int LinuxBaseEnv::channelCount()
{
   return 10000;  // TODO max? //(int)channels.size();
}
u64 LinuxBaseEnv::storageSize()
{
   u64 end_of_block_device = 0;
   raidCtl->forEach([&end_of_block_device]([[maybe_unused]]std::string& dev, int& fd) {
      u64 this_end = 0;
      ioctl(fd, BLKGETSIZE64, &this_end);
      if (end_of_block_device == 0) {
         end_of_block_device = this_end;
      }
      if (this_end != end_of_block_device) {
         std::cout << "WARNING: using SSD with different sizes. Using std::min(" << this_end << "," << end_of_block_device << ")" << std::endl;
         end_of_block_device = std::min(this_end, end_of_block_device);
      }
   });
   return end_of_block_device;
};
// -------------------------------------------------------------------------------------
RaidController<int>& LinuxBaseEnv::getRaidCtl()
{
   return *raidCtl;
}
DeviceInformation LinuxBaseEnv::getDeviceInfo() {
   DeviceInformation d;
   d.devices.resize(deviceCount());
   for (int i = 0; i < raidCtl->deviceCount(); i++) {
      d.devices[i].id = i;
      d.devices[i].name = raidCtl->name(i);
   }
   return d;
}
// -------------------------------------------------------------------------------------
// Linux Base Channel
// -------------------------------------------------------------------------------------
LinuxBaseChannel::LinuxBaseChannel(RaidController<int>& raidCtl, IoOptions ioOptions) : raidCtl(raidCtl), ioOptions(ioOptions) {
}
void* LinuxBaseEnv::allocIoMemoryChecked(size_t size, size_t align)
{
   auto mem = allocIoMemory(size, align);
   null_check(mem, "Memory allocation failed");
   return mem;
};
void* LinuxBaseEnv::allocIoMemory(size_t size, [[maybe_unused]]size_t align)
{
   void* bfs = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
   madvise(bfs, size, MADV_HUGEPAGE);
   madvise(bfs, size,
           MADV_DONTFORK);  // O_DIRECT does not work with forking.
   return bfs;
   // return std::aligned_alloc(align, size);
}
void LinuxBaseEnv::freeIoMemory(void* ptr, size_t size)
{
   munmap(ptr, size);
   // std::free(ptr);
}
void LinuxBaseChannel::pushBlocking(IoRequestType type, char* data, s64 addr, u64 len, [[maybe_unused]] bool write_back)
{
	int* fd;
	u64 raidedOffset;
	raidCtl.calc(addr, len, fd, raidedOffset);
   switch (type) {
      case IoRequestType::Read: {
         s64 ok = pread(*fd, data, len, raidedOffset);
         posix_check(ok);
         ensure(ok == (s64)len);
         break;
      }
      case IoRequestType::Write: {
         s64 ok = pwrite(*fd, data, len, raidedOffset);
         posix_check(ok);
         ensure(ok == (s64)len);
         break;
      }
      default:
         throw std::logic_error("not implemented");
   }
}
// -------------------------------------------------------------------------------------
// Libaio Env
// -------------------------------------------------------------------------------------
LibaioChannel& LibaioEnv::getIoChannel(int channel)
{
   auto ch = channels.find(channel);
   if (ch == channels.end()) {
      // create new
#ifdef MEAN_USE_THREADING
      if (channel > 40) {
         ioOptions.iodepth = 1;
      }
#endif
      ch = channels.insert({channel, std::unique_ptr<LibaioChannel>(new LibaioChannel(*raidCtl, ioOptions))}).first;
   }
   return *ch->second;
}
// -------------------------------------------------------------------------------------
// Libaio Channel
// -------------------------------------------------------------------------------------
LibaioChannel::LibaioChannel(RaidController<int>& raidCtl, IoOptions ioOptions) : LinuxBaseChannel(raidCtl, ioOptions)
{
   // -------------------------------------------------------------------------------------
   memset(&aio_context, 0, sizeof(aio_context));
   const int ret = io_setup(ioOptions.iodepth, &aio_context);
   if (ret != 0) {
      throw std::logic_error("io_setup failed, ret code = " + std::to_string(ret) + 
            "\nIff ret code is 11/EAGAIN, this could be cause by a too high iodepth. " + 
            "See: /proc/sys/fs/aio-max-nr for max number of aio events that can be used.");
   }
   // -------------------------------------------------------------------------------------
   request_stack.reserve(ioOptions.iodepth);
   events = std::make_unique<struct io_event[]>(ioOptions.iodepth);
}
// -------------------------------------------------------------------------------------
LibaioChannel::~LibaioChannel()
{
   io_destroy(aio_context);
}
// -------------------------------------------------------------------------------------
void LibaioChannel::_push(RaidRequest<LibaioIoRequest>* req)
{
   switch (req->base.type) {
   case IoRequestType::Write:
      io_prep_pwrite(&req->impl.aio_iocb, raidCtl.device(req->base.device), req->base.buffer(), req->base.len, req->base.offset);
      break;
   case IoRequestType::Read:
      io_prep_pread(&req->impl.aio_iocb, raidCtl.device(req->base.device), req->base.buffer(), req->base.len, req->base.offset);
      // std::cout << "read: " << req->aio_fildes << " len: " << req->u.c.nbytes << " addr: " << req->u.c.offset << std::endl;
      break;
   default:
      throw "";
   }
   request_stack.push_back(reinterpret_cast<iocb*>(req)); 
}
// -------------------------------------------------------------------------------------
void LibaioChannel::_printSpecializedCounters(std::ostream& ss)
{
   ss << "aio: ";
}
// -------------------------------------------------------------------------------------
int LibaioChannel::_submit()
{
   /*
   if(rand() % 1000000 == 0)
      printf("len: %i thr: %p init: %p cnt: %lu \n", request_stack.data()[0]->u.saddr.len, (void*)pthread_self(), &aio_context, request_stack.size());
      */
   int submitted = io_submit(aio_context, request_stack.size(), reinterpret_cast<iocb**>(request_stack.data()));
   ensure(request_stack.size() == (u64)submitted);
   outstanding += request_stack.size();
   request_stack.clear();
   return submitted;
}
// -------------------------------------------------------------------------------------
int LibaioChannel::_poll(int)
{
   //ensure(outstanding <= ioOptions.iodepth);
   int done_requests = 0;
   do {
      done_requests = io_getevents(aio_context, 0, outstanding, events.get(), NULL);
   } while (done_requests == -EINTR); // interrupted by user, e.g. in gdb
   ensure(done_requests >= 0);
   outstanding -= done_requests;
   // std::cout << "polled: outstanding: " << request_stack->outstanding() << " done: " << done_requests << " outstanding: " <<
   // request_stack->outstanding() << std::endl << std::flush; ensure(done_requests == request_stack->outstanding());
   for (int i = 0; i < done_requests; i++) {
      auto& event = events[i];
      ensure(event.res2 == 0);
      // ensure(event.res == ioOptions.write_back_buffer_size);
      auto req = reinterpret_cast<RaidRequest<LibaioIoRequest>*>(event.obj);
      if (event.res != req->base.len) {
         req->base.print(std::cout);
         throw std::logic_error("libaio event.res != len: event.res: " + std::to_string((long)event.res) + " len: " + std::to_string(req->base.len));
      }
      // eunsure(((char*)req->u.c.buf)[0] == (char)(req->u.c.offset/(16*1024)));
      req->base.innerCallback.callback(&req->base);
      // std::cout << "poll done " << event.res << "r2: " << event.res2 << " time: " <<
      // std::chrono::duration_cast<std::chrono::microseconds>(req->base.completion_time - req->base.push_time).count() << std::endl;
   }
   return done_requests;
}
/*
   u64 LibaioChannel::_readSync(char* destination, u64 len, u64 addr) override {
   LEANSTORE_BLOCK( auto start = std::chrono::high_resolution_clock::now(); );
  s64bytes_left = len;
   do {
   const int bytes_read = pread(fd, destination, len, addr);
   if (bytes_read != (s64)len) throw std::logic_error("pread did not read required length");
   assert(bytes_left > 0);
   bytes_left -= bytes_read;
   } while (bytes_left > 0);
   LEANSTORE_BLOCK(
   auto done = std::chrono::high_resolution_clock::now();
   auto& cnters = WorkerCounters::myCounters();
   auto diff  = std::chrono::duration_cast<std::chrono::nanoseconds>(done - start).count();
   cnters.read_push_done_latency += diff;
   cnters.read_push_done_hist.increaseSlot(diff/1000);
   cnters.reads++;
   });
   return len;
   }

   int Libaio::_syncReadsPollServer() override {
   throw std::logic_error("not needed for libaio");
   }

   void Libaio::_fDataSync() override {
   fdatasync(fd);
   }
   */
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
