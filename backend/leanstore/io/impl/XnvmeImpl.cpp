#include <stdexcept>
#include <string>
#include <filesystem>
#include <regex>
#include "leanstore/io/IoRequest.hpp"

#ifdef LEANSTORE_INCLUDE_XNVME

#include "XnvmeImpl.hpp"

#include "libxnvme_nvm.h"
#include <libxnvme_adm.h>
#include "libxnvmec.h"
// -------------------------------------------------------------------------------------
#include <cstring>

#define IOCOUNTERS

namespace mean
{
// -------------------------------------------------------------------------------------
XnvmeEnv::~XnvmeEnv()
{
   std::cout << "XnvmeEnv deinit" << std::endl;
   for (auto& chp: channels) {
      chp.reset(nullptr);
   }
   raidCtl->forEach([]([[maybe_unused]]std::string& dev, XnvmeDevQueues& fd) {
      xnvme_dev_close(fd.dev);
      fd.dev = nullptr;
   });
}
// -------------------------------------------------------------------------------------
void XnvmeEnv::init(IoOptions options)
{
   this->ioOptions = options;
   raidCtl = std::make_unique<RaidController<XnvmeDevQueues>>(ioOptions.path);
   raidCtl->forEach([this](std::string& dev, XnvmeDevQueues& fd) {
      xnvme_opts ops = xnvme_opts_default();
      std::string e = ioOptions.engine;
      if (e.find(":") != std::string::npos) {
         e = e.substr(e.find(":")+1, e.length());
         std::cout << "eng: " << e << std::endl;
         if (e == "spdk") {
            ops.be = "spdk";
         } else if (e == "io_uring_cmd") {
            // io_uring_cmd uses the character device, e.g. /dev/ng0n1
            // for safety reasons however we use block deivce names, e.g. /dev/nvme0n1 or /dev/disk/by-id/...
            // hence we have to follow the symlinks until we reach  the nvme0n1 device and translate it to the corresponding character device
            std::filesystem::path p = dev;
            std::cout << "dev: " << dev;
            while (std::filesystem::is_symlink(p)) {
               p = std::filesystem::read_symlink(p);
               std::cout << " -> " << p;
            }
            std::cout << std::endl;
            if (p.u8string().find("nvme") == std::string::npos &&
               p.u8string().find("ng") == std::string::npos) {
               ensure(false, "\"" + dev +"\" not a block or character device");
            }
            std::regex r("nvme");
            dev = std::regex_replace(p.u8string(), r, "ng");
            std::regex r2("../../");
            dev = std::regex_replace(dev, r2, "/dev/");
            std::cout << "dev=" << dev << std::endl;
            ops.be = "linux";
            ops.async = e.c_str();
         } else {
            ops.be = "linux";
            ops.async = e.c_str();
         }
      }
      ops.create = false;
      ops.direct = true;
      fd.dev = xnvme_dev_open(dev.c_str(), &ops);
      if (!fd.dev) {
         throw std::logic_error("xnvme init failed. Are you using the right device and is it initalized? errno: " + std::to_string(errno));
      }
      fd.nsid = xnvme_dev_get_nsid(fd.dev);
      if (!fd.nsid) {
         throw std::logic_error("xnvme init nsid failed.  " + std::to_string(errno));
      }
      xnvme_dev_pr(fd.dev, XNVME_PR_DEF);

      // get number of queues
      struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(fd.dev);
      struct xnvme_spec_feat feat = { .val = 0 };
      int err = xnvme_adm_gfeat(&ctx, 0x0, XNVME_SPEC_FEAT_NQUEUES,
            XNVME_SPEC_FEAT_SEL_CURRENT, NULL, 0);
      if (err || xnvme_cmd_ctx_cpl_status(&ctx)) {
         xnvmec_perr("xnvme_adm_gfeat()", err);
         xnvme_cmd_ctx_pr(&ctx, XNVME_PR_DEF);
         err = err ? err : -EIO;
         throw std::logic_error("xnvme error: " +std::to_string(err));
      }
      feat.val = ctx.cpl.cdw0;
      ensure(feat.nqueues.nsqa >= feat.nqueues.ncqa);
      int qs = feat.nqueues.nsqa;

      std::cout << "Initializing " << qs << " queues" << std::endl;
      fd.queues.resize(qs);
      for (int i = 0; i < qs; i++) {
         int err = xnvme_queue_init(fd.dev, ioOptions.iodepth, 0, &fd.queues[i]);
         if (err) {
            throw std::logic_error("xnvme_queue_init() failed. err: " + std::to_string(err) + "(e.g.: 22: is iodepth a power of 2? iod: " + std::to_string(ioOptions.iodepth) + "; 11: too high iodepth) ");
         }
         // we use cmd_ctx callbacks. This is just a "sane" default
         xnvme_queue_set_cb(fd.queues[i], nullptr, nullptr);
         //printf("Read uri: '%s', qd: %d", cli->args.uri, qd);
         //xnvme_lba_range_pr(&rng, XNVME_PR_DEF);
         channels.push_back(std::make_unique<XnvmeChannel>(ioOptions, *raidCtl, i, *this));
      }
   });
}

XnvmeChannel& XnvmeEnv::getIoChannel(int channel)
{
   ensure(channels.size() > 0, "don't forget to initizalize the io env first");
   // std::cout << "getChannel: " << channel << std::endl << std::flush;
   return *channels.at(channel);
}
int XnvmeEnv::deviceCount()
{
   return raidCtl->deviceCount();
}

int XnvmeEnv::channelCount()
{
   return 1000;///controller->qpairSize();
}

u64 XnvmeEnv::storageSize()
{
   return 10000000000;//controller->nsSize();
}

void* XnvmeEnv::allocIoMemory(size_t size, [[maybe_unused]]size_t align)
{
   return xnvme_buf_alloc(raidCtl->device(0).dev, size);
}

void* XnvmeEnv::allocIoMemoryChecked(size_t size, size_t align)
{
   auto mem = allocIoMemory(size, align);
   null_check(mem, "Memory allocation failed");
   return mem;
}

void XnvmeEnv::freeIoMemory(void* ptr, [[maybe_unused]]size_t size)
{
   xnvme_buf_free(raidCtl->device(0).dev, ptr);
}

DeviceInformation XnvmeEnv::getDeviceInfo() {
   DeviceInformation d;
   d.devices.resize(deviceCount());
   for (int i = 0; i < raidCtl->deviceCount(); i++) {
      d.devices[i].id = i;
      d.devices[i].name = raidCtl->name(i);
   }
   return d;
}

// -------------------------------------------------------------------------------------
XnvmeChannel::XnvmeChannel(IoOptions ioOptions, RaidController<XnvmeDevQueues>& raidCtl, int queue, XnvmeEnv& env) : ioOptions(ioOptions), env(env), raidCtl(raidCtl), queue(queue)
{
}

XnvmeChannel::~XnvmeChannel()
{
}

void XnvmeChannel::prepare_request(RaidRequest<XnvmeRequest>* req)
{
   // base
}

void XnvmeChannel::_push(RaidRequest<XnvmeRequest>* req)
{
   prepare_request(req);
   write_request_stack.push_back(req);
}

template<typename SubmissionFun>
void XnvmeChannel::submission(SubmissionFun subFun, RaidRequest<XnvmeRequest>* req) {
   constexpr auto xnvmeCb = [](struct xnvme_cmd_ctx *ctx, void *opaque) {
      RaidRequest<XnvmeRequest>* cbReq = reinterpret_cast<RaidRequest<XnvmeRequest>*>(opaque);
      if (xnvme_cmd_ctx_cpl_status(ctx)) {
         xnvme_cmd_ctx_pr(ctx, XNVME_PR_DEF);
         ensure(false); 
      }
      cbReq->base.innerCallback.callback(&cbReq->base);
      xnvme_queue_put_cmd_ctx(ctx->async.queue, ctx);
   };
   XnvmeDevQueues* device;
   u64 raidedOffset;
   raidCtl.calc(req->base.addr, req->base.len, device, raidedOffset);
   int err = -1;
   struct xnvme_cmd_ctx *ctx = xnvme_queue_get_cmd_ctx(device->queues[queue]);
   ensure(ctx);
   xnvme_cmd_ctx_set_cb(ctx, xnvmeCb, req);
   const struct xnvme_geo* geo = xnvme_dev_get_geo(device->dev);
   //std::cout << "nsid: " << device->nsid <<  " raidedOffset lbas: " << raidedOffset / geo->nbytes << " len: " << req->len / geo->nbytes << " "<< std::endl;
   err = subFun(ctx, device->nsid, raidedOffset / geo->nbytes, req->base.len / geo->nbytes -1 /*zero based value @,@*/ , req->base.data, nullptr);
   if (err != 0) {
      xnvmec_perr("submission-error", err);
      xnvme_queue_put_cmd_ctx(device->queues[queue], ctx);
      ensure(false); 
   }
};

int XnvmeChannel::_submit()
{
   for (auto req: write_request_stack) {
      switch (req->base.type) {
         case IoRequestType::Read:
            //std::cout << "read submit offset: " << req->addr << " len: " << req->len << std::endl;
            submission(xnvme_nvm_read,req);
            break;
         case IoRequestType::Write:
            //std::cout << "write submit offset: " << req->addr << " len: " << req->len << std::endl;
            submission(xnvme_nvm_write,req);
            break;
         default:
            throw std::logic_error("Not supported");
      }
   }
   int submitted = write_request_stack.size();
   write_request_stack.clear();
   return submitted;
}

int XnvmeChannel::_poll(int)
{
   //int xnvme_queue_poke(struct xnvme_queue * queue, uint32_t max)
   int done = 0;
   raidCtl.forEach([&done, queue = this->queue]([[maybe_unused]]std::string& dev, XnvmeDevQueues& device) {
         int ret = xnvme_queue_poke(device.queues[queue], 0);
         ensure(ret >= 0);
         done += ret;
   });
   return done;
}

void XnvmeChannel::_printSpecializedCounters(std::ostream& ss)
{
   ss << "xnvme: ";
}
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
//
#endif // LEANSTORE_INCLUDE_XNVME
