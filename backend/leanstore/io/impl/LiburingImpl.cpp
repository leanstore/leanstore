#include "LiburingImpl.hpp"

// -------------------------------------------------------------------------------------
#include "../IoInterface.hpp"
#include "Exceptions.hpp"
#include "leanstore/io/IoRequest.hpp"
#include "liburing.h"
#include "liburing/io_uring.h"
// -------------------------------------------------------------------------------------
#include <libnvme.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <nvme/api-types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace mean
{
static int nvme_identify(int fd, __u32 nsid, enum nvme_identify_cns cns,
			 enum nvme_csi csi, void *data)
{
	struct nvme_passthru_cmd cmd = {
		.opcode         = nvme_admin_identify,
		.nsid           = nsid,
		.addr           = (__u64)(uintptr_t)data,
		.data_len       = NVME_IDENTIFY_DATA_SIZE,
		.cdw10          = cns,
		.cdw11          = (uint32_t)csi << 24,
		.timeout_ms     = NVME_DEFAULT_IOCTL_TIMEOUT,
	};

	return ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd);
}
[[maybe_unused]]
static int nvme_get_info(int fd, __u32 *nsid, __u32 *lba_sz, __u64 *nlba)
{
	struct nvme_id_ns ns;
	int namespace_id;
	int err;

	namespace_id = ioctl(fd, NVME_IOCTL_ID);
	if (namespace_id < 0) {
		fprintf(stderr, "error failed to fetch namespace-id\n");
		close(fd);
		return -errno;
	}

	/*
	 * Identify namespace to get namespace-id, namespace size in LBA's
	 * and LBA data size.
	 */
	err = nvme_identify(fd, namespace_id, NVME_IDENTIFY_CNS_NS,
				NVME_CSI_NVM, &ns);
	if (err) {
		fprintf(stderr, "error failed to fetch identify namespace\n");
		close(fd);
		return err;
	}

	*nsid = namespace_id;
	*lba_sz = 1 << ns.lbaf[(ns.flbas & 0x0f)].ds;
	*nlba = ns.nsze;

	return 0;
}

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
   struct nvme_id_ns ns;
   nvme_identify(raidCtl.device(0), 1, NVME_IDENTIFY_CNS_NS, NVME_CSI_NVM, &ns);
	lba_sz = 1 << ns.lbaf[(ns.flbas & 0x0f)].ds;
   
   io_uring_params iouParameters;
   memset(&iouParameters, 0, sizeof(iouParameters));
	if (ioOptions.ioUringNVMePassthrough) {
		iouParameters.flags |= IORING_SETUP_SQE128;
		iouParameters.flags |= IORING_SETUP_CQE32;
	}
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
[[maybe_unused]]
static void print_sqe(struct io_uring_sqe *sqe) {
	struct nvme_uring_cmd *cmd = (struct nvme_uring_cmd *)&sqe->cmd;
   printf("sqe: opcode: %i flags: %i  ioprio: %i fd: %i off: %llu (cmd_op: %u) addr: %llu len: %i uring_cmd: %i  user_data: %llu buf_group: %i personality: %i splic: %i ", sqe->opcode, sqe->flags, sqe->ioprio, sqe->fd,
          sqe->off,
          sqe->cmd_op,
          sqe->addr,
          sqe->len,
          sqe->uring_cmd_flags,
          sqe->user_data,
          sqe->buf_group,
          sqe->personality,
          sqe->splice_fd_in);
   printf("cmd: opcode: %i flags: %i rsvd1: %i nsid: %i cdw2: %i cdw3: %i meta: %llu addr: %llu meta_len: %i data_len: %i cdw10-15 %u %u %u %u %u %u timeout: %i rsvd: %i \n",
          cmd->opcode, cmd->flags, cmd->rsvd1, cmd->nsid, cmd->cdw2, cmd->cdw3, cmd->metadata, cmd->addr, cmd->metadata_len, cmd->data_len, cmd->cdw10, cmd->cdw11, cmd->cdw12, cmd->cdw13, cmd->cdw14, cmd->cdw15, cmd->timeout_ms, cmd->rsvd2);
}
// -------------------------------------------------------------------------------------
void prep_uring_cmd(uint8_t opcode, struct io_uring_sqe* sqe, int fd, struct iovec* iov, uint64_t slba, uint64_t nlb) {
   sqe->opcode = IORING_OP_URING_CMD;
   sqe->fd = fd;
   sqe->flags = 0;
   sqe->user_data = 0;
   sqe->cmd_op = NVME_URING_CMD_IO;
   struct nvme_uring_cmd* cmd = (struct nvme_uring_cmd *)&sqe->cmd;
   /* cdw10 and cdw11 represent starting slba*/
   cmd->cdw10 = slba & 0xffffffff;
   cmd->cdw11 = slba >> 32;
   /* cdw12 represent number of lba to be read*/
   cmd->cdw12 = nlb - 1;
   cmd->addr = (unsigned long) iov[0].iov_base;
   cmd->data_len = iov[0].iov_len ;
   cmd->nsid = 1; // TODO
   cmd->opcode = opcode;
   cmd->cdw13 = 1 << 6; // DSM Sequential Request
}
// -------------------------------------------------------------------------------------
int LiburingChannel::_submit()
{
   unsigned submitted = 0;
   int reads = 0;
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
            if (ioOptions.ioUringNVMePassthrough) {
               prep_uring_cmd(nvme_cmd_write, sqe, *fd, &req->impl.iov, raidedOffset / lba_sz, req->impl.iov.iov_len/lba_sz);
            } else {
               io_uring_prep_writev(sqe, *fd, &req->impl.iov, 1, raidedOffset);
            }
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
            if (ioOptions.ioUringNVMePassthrough) {
               prep_uring_cmd(nvme_cmd_read, sqe, *fd, &req->impl.iov, raidedOffset / lba_sz, req->impl.iov.iov_len/lba_sz);
            } else {
               io_uring_prep_readv(sqe, *fd, &req->impl.iov, 1, raidedOffset);
            }
            reads++;
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
   if (reads > 0) {
      leanstore::WorkerCounters::myCounters().submit_calls++;
      leanstore::WorkerCounters::myCounters().submitted.fetch_add(reads);
   }
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
         [[maybe_unused]] int submitted = io_uring_submit(&ring); // enter kernel for polling
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
      if ((ioOptions.ioUringNVMePassthrough && cqe->res != 0) || (!ioOptions.ioUringNVMePassthrough && cqe->res != (s32)req->base.len)) {
         [[maybe_unused]] auto r = cqe->res;
         [[maybe_unused]] auto d = reinterpret_cast<LiburingIoRequest*>(io_uring_cqe_get_data(cqe));
         req->base.print(std::cout);
         throw std::logic_error("liburing io failed cqe->res != len. res: " + std::to_string((long)cqe->res) +
                                " len: " + std::to_string(req->base.len));
      }
      // std::cout << "res: " << cqe->res << std::endl;
      done++;
      // std::cout << "read ok: " << req->base.data << " len: " << req->base.len << " off: " << req->base.addr << std::endl;
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
