#include "LiburingImpl.hpp"

// -------------------------------------------------------------------------------------
#include "../IoInterface.hpp"
#include "Exceptions.hpp"
#include "leanstore/io/IoRequest.hpp"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
// Env
// -------------------------------------------------------------------------------------
LiburingChannel& LiburingEnv::getIoChannel(int channel)
{
   auto ch = channels.find(channel);
   if (ch == channels.end()) {
      // create new
      ch = channels.insert({channel, std::unique_ptr<LiburingChannel>(new LiburingChannel(*raidCtl, ioOptions, *this))}).first;
   }
   return *ch->second;
}
// -------------------------------------------------------------------------------------
// Channel
// -------------------------------------------------------------------------------------
LiburingChannel::LiburingChannel(RaidController<int>& raidCtl, IoOptions ioOptions, LiburingEnv& env) : LinuxBaseChannel(raidCtl, ioOptions)
{
   // int ret = io_uring_queue_init(ioOptions.iodepth, &ring, IORING_SETUP_IOPOLL);
   io_uring_params iouParameters;
   memset(&iouParameters, 0, sizeof(iouParameters));
   iouParameters.flags |= ioOptions.ioUringPollMode ? IORING_SETUP_IOPOLL : 0;
   //iouParameters.flags |= IORING_FEAT_NATIVE_WORKERS;
   iouParameters.flags |= ioOptions.ioUringShareWq > 0 ? IORING_SETUP_SQPOLL : 0;
   if (ioOptions.ioUringShareWq > 0 && env.channels.size() >= (unsigned)ioOptions.ioUringShareWq) {
      iouParameters.flags |= IORING_SETUP_ATTACH_WQ;
      // round robin the wq's
      iouParameters.wq_fd = dynamic_cast<LiburingChannel*>(env.channels[ env.channels.size() % ioOptions.ioUringShareWq ].get())->ring.ring_fd;
   }
   std::cout << "io_uring parameters: sq_entries: " << iouParameters.sq_entries << " cq_entries: " << iouParameters.cq_entries 
      << " flags: " << iouParameters.flags << " sq_thread_cpu: " << iouParameters.sq_thread_cpu << " sq_thread_idle: " << iouParameters.sq_thread_idle 
      << " features: " << iouParameters.features << " wq_fd: "<<iouParameters.wq_fd << std::endl;
   int ret = io_uring_queue_init_params(ioOptions.iodepth, &ring, &iouParameters);
   if (ret < 0) {
      throw std::logic_error("io_uring_queue_init failed, ret code = " + std::to_string(ret));
   }
   // -------------------------------------------------------------------------------------
   request_stack.reserve(ioOptions.iodepth);
}
// -------------------------------------------------------------------------------------
LiburingChannel::~LiburingChannel()
{
   io_uring_queue_exit(&ring);
}
// -------------------------------------------------------------------------------------
/*
int Liburing::_spaceForPush() override {
        return request_stack->free;
}*/
// -------------------------------------------------------------------------------------
void LiburingChannel::_push(RaidRequest<LiburingIoRequest>* req)
{
   request_stack.push_back(req); 
}
// -------------------------------------------------------------------------------------
int LiburingChannel::_submit()
{
   unsigned submitted = 0;
   for (unsigned i = 0; i < request_stack.size(); i++) {
      auto req = request_stack[i];
      // std::cout << "submit: " << i << " bf?: " << (void*)request_stack->submit_stack.get()[i]->base.user_data << std::endl << std::flush;
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      ensure(sqe);
      req->impl.iov.iov_base = req->base.data;  // hack does not work without writev/reqadv
      req->impl.iov.iov_len = req->base.len;
      switch (req->base.type) {
         case IoRequestType::Write: {
            int* fd;
            u64 raidedOffset;
            raidCtl.calc(req->base.addr, req->base.len, fd, raidedOffset);
            auto dataBuf = req->base.data;
            if (req->base.write_back) {
               assert(req->base.len <= ioOptions.write_back_buffer_size);
               std::memcpy(req->base.write_back_buffer, dataBuf, req->base.len);
               req->impl.iov.iov_base = req->base.write_back_buffer;  // writev/readv
               dataBuf = req->base.write_back_buffer;
            }
            assert((uintptr_t)req->base.data % 512 == 0);
            // io_uring_prep_write(sqe, fd, dataBuf, req->base.len, req->base.addr);
            // std::cout << "write: buf: " << sqe->addr << " len: " << sqe->len << " addr: " << sqe->off << std::endl;
            io_uring_prep_writev(sqe, *fd, &req->impl.iov, 1, raidedOffset);
            // std::cout << "write: " << req->iov.iov_base << " len: " << req->iov.iov_len << " addr: " << req->base.addr << std::endl;
            break;
         }
         case IoRequestType::Read: {
            int* fd;
            u64 raidedOffset;
            raidCtl.calc(req->base.addr, req->base.len, fd, raidedOffset);
            assert((uintptr_t)req->base.data % 512 == 0);
            // io_uring_prep_read(sqe, fd, req->base.data, req->base.len, req->base.addr);
            // std::cout << "read buf: " << sqe->addr << " len: " << sqe->len << " off: " << sqe->off << std::endl;
            io_uring_prep_readv(sqe, *fd, &req->impl.iov, 1, raidedOffset);
            // std::cout << "read: " << req->iov.iov_base << " len: " << req->iov.iov_len << " addr: " << req->base.addr << std::endl;
            break;
         }
         default:
            throw std::logic_error("IoRequestType not supported");
      }
      io_uring_sqe_set_data(sqe, req);
   }
   // LEANSTORE_BLOCK( PPCounters::myCounters().io_submits++; )
   submitted = io_uring_submit(&ring);
   // std::cout << "submit: pushed: " << request_stack->pushed	<< " submitted: " << submitted << " outstanding: " <<
   // request_stack->outstanding()	<< std::endl << std::flush;
   //ensure(request_stack.size() == submitted);
   request_stack.clear();
   return submitted;
}
// -------------------------------------------------------------------------------------
int LiburingChannel::_poll(int)
{
   int done = 0;
   if (ioOptions.ioUringPollMode) {  // prohibits never entering the kernel when nothing is submitted.
      //int submitted = io_uring_submit(&ring);
   }
   struct io_uring_cqe* cqe = nullptr;
   while (true) {
      //int ok = io_uring_peek_cqe(&ring, &cqe);
      unsigned int avail = 0;
      // lib-internal call, doesn't actually poll/enter the kernel, only peaks cq.
      int ok = __io_uring_peek_cqe(&ring, &cqe, &avail);
      posix_check(ok == 0 || ok == -EAGAIN);
      if (!cqe || ok == -EAGAIN) {
         // prohibit starving from not actually polling io
         int submitted = io_uring_submit(&ring); // enter kernel for polling
         assert(submitted==0);
         int ok = __io_uring_peek_cqe(&ring, &cqe, &avail);
         posix_check(ok == 0 || ok == -EAGAIN);
         if (!cqe || ok == -EAGAIN) {
            break;
         }
      }
      /*
      if (!cqe) {
         throw "";
      }
      */
      auto req = reinterpret_cast<RaidRequest<LiburingIoRequest>*>(io_uring_cqe_get_data(cqe));
      if (cqe->res != (s32)req->base.len) {
         [[maybe_unused]] auto r = cqe->res;
         [[maybe_unused]] auto d = reinterpret_cast<LiburingIoRequest*>(io_uring_cqe_get_data(cqe));
         req->base.print(std::cout);
         throw std::logic_error("liburing io failed cqe->res != len. res: " + std::to_string((long)cqe->res) +
                                " len: " + std::to_string(req->base.len));
      }
      // std::cout << "res: " << cqe->res << std::endl;
      done++;
      // std::cout << "read ok: " << req->base.data << " len: " << req->base.len << " off: " << req->base.addr << std::endl;
      ensure((int)req->impl.iov.iov_len == cqe->res);
      io_uring_cqe_seen(&ring, cqe);
      req->base.innerCallback.callback(&req->base);
   }
   return done;
}
// -------------------------------------------------------------------------------------
void LiburingChannel::_printSpecializedCounters(std::ostream& ss)
{
   ss << "uring: ";
}
// -------------------------------------------------------------------------------------
/*
u64 Liburing::_readSync(char* destination, u64 len, u64 addr) override {
        LEANSTORE_BLOCK( auto start = std::chrono::high_resolution_clock::now(); );
        i64 bytes_left = len;
        do {
                const int bytes_read = pread(fd, destination, len, addr);
                if (bytes_read != (i64)len) throw std::logic_error("pread did not read required length");
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
        throw std::logic_error("not implemented");
}

int Liburing::_syncReadsPollServer() override {
        throw std::logic_error("not needed for libaio");
}

void Liburing::_fDataSync() override {
        fdatasync(fd);
}
*/
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
//
