#include "IoInterface.hpp"
#include <linux/fs.h>
#include <memory>
#include <stdexcept>
// -------------------------------------------------------------------------------------
#include "impl/LibaioImpl.hpp"
#include "impl/LiburingImpl.hpp"
#include "impl/SpdkImpl.hpp"
#include "impl/XnvmeImpl.hpp"
// -------------------------------------------------------------------------------------
namespace mean
{
std::unique_ptr<RaidEnvironment> IoInterface::_instance = nullptr;
// -------------------------------------------------------------------------------------
RaidEnvironment& IoInterface::initInstance(IoOptions ioOptions)
{
   if (ioOptions.engine == "libaio") {
      _instance = std::unique_ptr<RaidEnvironment>(new RaidEnv<LibaioEnv, LibaioChannel, LibaioIoRequest>(ioOptions));
   } else if (ioOptions.engine == "spdk") {
      _instance = std::unique_ptr<RaidEnvironment>(new RaidEnv<SpdkEnv, SpdkChannel, SpdkIoReq>(ioOptions));
   } else if (ioOptions.engine == "io_uring") {
      _instance = std::unique_ptr<RaidEnvironment>(new RaidEnv<LiburingEnv, LiburingChannel, LiburingIoRequest>(ioOptions));
#ifdef LEANSTORE_INCLUDE_XNVME
   } else if (ioOptions.engine.find("xnvme") != string::npos) {
      _instance = std::unique_ptr<RaidEnvironment>(new RaidEnv<XnvmeEnv, XnvmeChannel, XnvmeRequest>(ioOptions));
#endif
   } else {
      throw std::logic_error("not implemented");
   }
   return *_instance;
}
RaidEnvironment& IoInterface::instance()
{
   ensure(_instance.get(), "IoEnvironment not initialized.");
   return *_instance;
}
int IoInterface::channelCount() {
   return instance().channelCount();
}
IoChannel& IoInterface::getIoChannel(int channel) {
   return instance().getIoChannel(channel);
}
void* IoInterface::allocIoMemoryChecked(size_t size, size_t align)
{
   return instance().allocIoMemoryChecked(size, align);
}
void IoInterface::freeIoMemory(void* ptr, size_t size)
{
   instance().freeIoMemory(ptr, size);
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
/*
RaidChannelManger::RaidChannelManger(IoOptions options) : ioOptions(options) {
   if (ioOptions.engine == "auto") {
      if (ioOptions.path.find("traddr") != std::string::npos) {
         ioOptions.engine = "spdk";
      } else {
         ioOptions.engine = "libaio";
      }
   }
   // -------------------------------------------------------------------------------------
   if (ioOptions.engine == "spdk") {
      io_env = std::make_unique<SpdkEnv>();
   } else if (ioOptions.engine == "libaio") {
      io_env = std::unique_ptr<LibaioEnv>(new LibaioEnv());
   } else if (ioOptions.engine == "liburing") {
      io_env = std::unique_ptr<LiburingEnv>(new LiburingEnv());
#ifdef LEANSTORE_INCLUDE_XNVME
   } else if (ioOptions.engine == "xnvme") {
      io_env = std::unique_ptr<XnvmeEnv>(new XnvmeEnv());
#endif // LEANSTORE_INCLUDE_XNVME
   } else {
      throw std::logic_error("ioEngine does not exist: " + ioOptions.engine);
   }
   io_env->init(ioOptions);
}
IoChannel& RaidChannelManger::getIoChannel(int channel) {
   auto f = channels.find(channel);
   if (f == channels.end()) {
      // TODO If...
      auto ch = std::unique_ptr<IoChannel>(new Raid5Channel<LibaioEnv, LibaioChannel, LibaioIoRequest>(io_env->getIoChannel(channel)));
      channels[channel] = std::move(ch);
      return *channels[channel];
   }
   return *f->second;
}
int RaidChannelManger::channelCount() {
   return io_env->channelCount();
}
u64 RaidChannelManger::storageSize() {
   return io_env->storageSize(); // FIXME RAID dependent..
}
*/
}  // namespace mean
// -------------------------------------------------------------------------------------
