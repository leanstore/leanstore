// -------------------------------------------------------------------------------------
#include "Env.hpp"

#include "leanstore/concurrency/Mean.hpp"
#include "leanstore/io/IoInterface.hpp"
#include "leanstore/concurrency/ThreadBase.hpp"
#include "leanstore/io/impl/Spdk.hpp"

#include <cstdint>
#include <thread>
#include <chrono>
#include <stdlib.h>
#include <iomanip>
#include <mutex>

using namespace mean;

int main(int , char** ) {
   std::cout << "start" << std::endl;
   std::string filename = getEnvRequired("FILENAME");
   std::cout << "FILENAME: " << filename << std::endl;

   // =================================================================================
   std::thread([]{
      SpdkEnvironment::init();
   }).join();
   std::unique_ptr<NVMeInterface> nvme = std::make_unique<NVMeRaid0>();
   nvme->connect(filename);
   //spdk_nvme_qprio qp = SPDK_NVME_QPRIO_MEDIUM;
   //nvme->allocateQPairs(2, qp);
   nvme->allocateQPairs();
   // =================================================================================
   //IoOptions ioOptions("spdk", filename);
   //IoInterface::initInstance(ioOptions);
   // =================================================================================
   u64 block = 400*1024*1024*1024ll;
   u64 count = 500;
   std::vector<char*> mem;
   std::cout << "alloc" << std::endl;
   for (u64 i = 0; i < count; i++) {
      std::cout << "i: " << i << " GB: " <<  block*i/1024/1024/1024 <<std::endl;
      //char* buf = (char*)IoInterface::instance().allocIoMemoryChecked(block, 128); //128
      char* buf = (char*)SpdkEnvironment::dma_malloc(block, 128);
      ensure(buf);
      mem.push_back(buf);
   }
   std::cout << "ok" << std::endl;

   return 0;
}
