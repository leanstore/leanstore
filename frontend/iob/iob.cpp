// -------------------------------------------------------------------------------------
#include "RequestGenerator.hpp"
#include "Env.hpp"

#include "Time.hpp"
#include "leanstore/concurrency/Mean.hpp"
#include "leanstore/io/IoInterface.hpp"
#include "leanstore/concurrency/ThreadBase.hpp"

#include <cstdint>
#include <thread>
#include <chrono>
#include <stdlib.h>
#include <iomanip>
#include <mutex>

using namespace mean;

static void initializeSSDIfNecessary(long maxPage, long bufSize, std::string init, int iodepth) {
   // check if necessary
   IoChannel& ioChannel = IoInterface::instance().getIoChannel(0);
   int initBufSize = 64*1024;
   char* buf = (char*)IoInterface::instance().allocIoMemoryChecked(initBufSize + 512, 512);
   std::cout << "128 b aling" << std::endl;
   memset(buf, 0, initBufSize);

   long iniOps = maxPage*bufSize / initBufSize +1; // +1 because you need to round up 
   bool forceInit = init == "yes";
   bool autoInit = init == "auto";
   bool disableCheck = init == "disable" || init == "no";

   int iniDoneCheck = true;
   if (!forceInit && !disableCheck) {
      // just a heurisitc: check first and last page
      ioChannel.pushBlocking(IoRequestType::Read, buf, 0 * bufSize, bufSize);
      const bool check1 = RequestGenerator::checkBuffer(buf, 0, bufSize);
      std::cout << "check 0: " << check1 << std::endl;
      iniDoneCheck &= check1;
      RequestGenerator::resetBufferChecks(buf, bufSize);
      ioChannel.pushBlocking(IoRequestType::Read, buf, maxPage*bufSize, bufSize);
      const bool check2 = RequestGenerator::checkBuffer(buf, maxPage*bufSize, bufSize);
      std::cout << "check " << maxPage*bufSize << ": " << check2 << std::endl;
      iniDoneCheck &= check2;
      std::cout << "init check: " << (iniDoneCheck ? "probably already done" : "not done yet") << std::endl;
      if (!autoInit && !iniDoneCheck && !disableCheck) {
         throw std::logic_error("Init not done yet. If you know what you are doing use INIT=auto or INIT=yes to force SSD initialization.");
      }
   }
   if (!disableCheck) {
      if (forceInit || !iniDoneCheck ) {
         auto start = getSeconds();
         std::cout << ">> init: " << std::endl << std::flush;
         std::cout << "init "<< iniOps*initBufSize/1024/1024/1024 << " GB iniOps: " << iniOps<< std::endl << std::flush;
         JobOptions initOptions;
         initOptions.name = "init";
         initOptions.ioPattern = JobOptions::IoPattern::Sequential;
         initOptions.bs = initBufSize;
         initOptions.filesize = iniOps*initBufSize; // make sure that maxPage * 4kB are at least written
         initOptions.io_size = iniOps*initBufSize;
         initOptions.writePercent = 1;
         initOptions.iodepth = iodepth;
         RequestGenerator init("", initOptions, IoInterface::instance().getIoChannel(0), 0);
         init.runIo();
         std::cout << std::endl;
         auto time = getSeconds() - start;
         std::cout << "init done: " << iniOps*initBufSize/time/MEBI << "MiB/s ops: " << iniOps << " time: " << time << std::endl << std::flush;
      }
      // check again
      RequestGenerator::resetBufferChecks(buf, bufSize);
      ioChannel.pushBlocking(IoRequestType::Read, buf, 0 * bufSize, bufSize);
      RequestGenerator::checkBufferThrow(buf, 0, bufSize);
      RequestGenerator::resetBufferChecks(buf, bufSize);
      ioChannel.pushBlocking(IoRequestType::Read, buf, maxPage*bufSize, bufSize);
      RequestGenerator::checkBufferThrow(buf, maxPage*bufSize, bufSize);
      // check all 
   }
   IoInterface::instance().freeIoMemory(buf);
}

class RequestGeneratorThread : public ThreadBase {
public:
   RequestGenerator gen;
   RequestGeneratorThread(JobOptions jobOptions, int thr) 
      : ThreadBase("gent", thr), 
      gen(std::to_string(thr), jobOptions, IoInterface::instance().getIoChannel(thr), thr) {
         //setCpuAffinityBeforeStart(thr);
   }
   int process() override {
      gen.runIo();
      return 0;
   }
   void stop() {
      gen.stopIo();
   }
};

int main(int , char** ) {
   std::cout << "start" << std::endl;
   //std::string filename = getEnv("FILENAME", "trtype=PCIe traddr=0000.03.00.0 ns=1");
   //std::string filename = getEnv("FILENAME", "trtype=PCIe traddr=0000.06.00.0 ns=1");
   //std::string filename = getEnv("FILENAME",   "trtype=PCIe traddr=0000.87.00.0 ns=1;trtype=PCIe traddr=0000.88.00.0 ns=1;trtype=PCIe traddr=0000.c1.00.0 ns=1;trtype=PCIe traddr=0000.c2.00.0 ns=1");
   std::string filename = getEnvRequired("FILENAME");
   const int threads = getEnv("THREADS",1);
   const int ioDepth = getEnv("IO_DEPTH", 128);
   const int runtimeLimit = getEnv("RUNTIME", 0);
   const int ioUringPollMode = getEnv("IOUPOLL", 0);
   long bufSize = getBytesFromString(getEnv("BS", "4K"));
   if (bufSize % 512 != 0) { throw std::logic_error("BS is not a multiple of 512"); }
   if (bufSize % 4096 != 0) { std::cout << "BS is not a multiple of 4096. Are you sure that is what you want?" << std::endl; }
   long filesize = getBytesFromString(getEnv("FILESIZE", "10G"));
   float ioSize = getBytesFromString(getEnv("IO_SIZE", "10G"));
   float writePercent = getEnv("RW", 0);
   //float io_depth = getEnv("IO_DEPTH", 1);
   long opsPerThread = ioSize / bufSize / threads;
   long maxPage = filesize/bufSize;

   std::string init = getEnv("INIT", "no");
   std::string ioEngine = getEnv("IOENGINE", "auto");

   if (runtimeLimit > 0) {
      ioSize = -1;
   }

   std::cout << "FILENAME: " << filename << std::endl;
   std::cout << "BS: " << bufSize << std::endl;
   std::cout << "FILESIZE: " << filesize/(float)GIBI << "GiB pages: " << maxPage << std::endl;
   std::cout << "IO_SIZE: " << opsPerThread*bufSize/(float)GIBI << "GiB pages: " << opsPerThread << " per thread" << std::endl;
   std::cout << "INIT: " << init << std::endl;
   std::cout << "IO_DEPTH: " << ioDepth << std::endl;
   std::cout << "IOENGING: " << ioEngine << std::endl;

   IoOptions ioOptions(ioEngine, filename);
   ioOptions.iodepth = ioDepth; 
   ioOptions.channelCount = threads;
   ioOptions.ioUringPollMode = ioUringPollMode;
   IoInterface::initInstance(ioOptions);

   initializeSSDIfNecessary(maxPage, bufSize, init, ioDepth);

   //unanlignedBench(filesize, bufSize, ops, 0);
   std::ofstream dump;
   dump.open("dump.csv", std::ios_base::app);
   JobOptions jobOptions;
   jobOptions.filesize = filesize;
   jobOptions.ioPattern = JobOptions::IoPattern::Random;
   jobOptions.disableChecks = init == "disable";
   //jobOptions.enableIoTracing = true;
   JobStats::IoStat::dumpIoStatHeader(dump, "iodepth, bs, io_alignment,");
   dump << std::endl;

   jobOptions.bs = bufSize;
   jobOptions.iodepth = ioDepth;
   jobOptions.ioPattern = JobOptions::IoPattern::Random;
   jobOptions.io_alignment = bufSize;
   jobOptions.io_size = ioSize;
   jobOptions.writePercent = writePercent;
   jobOptions.threads = threads;

   std::cout << jobOptions.print();

   std::vector<std::unique_ptr<RequestGeneratorThread>> threadVec;
   for (int thr = 0; thr < threads; thr++) {
      jobOptions.name = "gen " + std::to_string(thr);
      threadVec.emplace_back(std::move(std::make_unique<RequestGeneratorThread>(jobOptions, thr)));
   }
   std::this_thread::sleep_for(std::chrono::milliseconds(1));
   for (auto& t: threadVec) {
      t->start();
   }

   long maxRead = 0;
   if (runtimeLimit > 0) {
      auto start = getSeconds();
      std::this_thread::sleep_for(std::chrono::seconds(1));
      for (int time = 1; time < runtimeLimit; time++) {
         long sumRead = 0;
         for (int i = 0; i < threads; i++) {
            //IoInterface::instance().getIoChannel(i).printCounters(std::cout);
            //std::cout << std::endl << std::flush;
         }
         for (int i = 0; i < threads; i++) {
            auto& thr = threadVec[i];
            int s = thr->gen.stats.seconds;
            sumRead += thr->gen.stats.readsPerSecond[s-1];
            std::cout << s << " r" << i << ": " << thr->gen.stats.readsPerSecond[s-1]/1000 << "k ";
         }
         std::cout << " total: "<< sumRead/1000 << "k " << std::endl << std::flush;
         maxRead = std::max(maxRead, sumRead);
         auto now = getSeconds();
         std::this_thread::sleep_for(std::chrono::microseconds((long)((time + 1 - (now - start))*1e6)));
      }
      for (auto& t: threadVec) {
         t->stop();
      }
   }

   u64 reads = 0;
   u64 writes = 0;
   u64 r50p = 0;
   u64 r99p = 0;
   u64 r99p9 = 0;
   u64 w50p = 0;
   u64 w99p = 0;
   u64 w99p9 = 0;
   u64 rTotalTime = 0;
   u64 wTotalTime = 0;
   float totalTime = 0;
   for (auto& t: threadVec) {
      t->join();
      t->gen.stats.printStats(std::cout);
      reads += t->gen.stats.reads;
      writes += t->gen.stats.writes;
      totalTime += t->gen.stats.time;
      r50p += t->gen.stats.readHist.getPercentile(50);
      r99p += t->gen.stats.readHist.getPercentile(99);
      r99p9 += t->gen.stats.readHist.getPercentile(99.9);
      w50p += t->gen.stats.writeHist.getPercentile(50);
      w99p += t->gen.stats.writeHist.getPercentile(99);
      w99p9 += t->gen.stats.writeHist.getPercentile(99.9);
      rTotalTime += t->gen.stats.readTotalTime;
      wTotalTime += t->gen.stats.writeTotalTime;
      t->gen.stats.dumpIoTrace(dump, std::to_string(jobOptions.iodepth) + "," + std::to_string(jobOptions.bs) + "," + std::to_string(jobOptions.io_alignment) + ",");
      std::cout << std::endl;
   }
   totalTime /= threads;
   r50p /= threads; r99p /= threads; r99p9 /= threads;
   w50p /= threads; w99p /= threads; w99p9 /= threads;
   dump << "filesize,io_size,filename,bs,rw,threads,iodepth,reads,writes,rmb,wmb,ravg,wavg,r50p,r99p,r99p9,w50p,w99p,w99p9"  << std::endl;
   dump << filesize << "," << ioSize << ","; 
   dump << "\""<< filename << "\"," << bufSize << "," << writePercent << "," << threads << "," << jobOptions.iodepth << ",";
   dump << reads/totalTime << "," << writes/totalTime << "," << reads/totalTime*bufSize/MEBI << "," << writes/totalTime *bufSize/MEBI<< ",";
   dump <<  std::setprecision(6) << (float)reads/rTotalTime*1e6 << "," << (float)writes/wTotalTime*1e6 << "," << r50p << "," << r99p << ","<< r99p9 << ","<< w50p << ","<< w99p << ","<< w99p9 << "," << std::endl;
   dump.close();
   std::cout << "summary ";
   std::cout << std::setprecision(4);
   std::cout <<  "[Miops] total: " << (reads+writes)/totalTime/MEGA; 
   std::cout << " reads: " << reads/totalTime/MEGA << " write: " << writes/totalTime/MEGA;
   std::cout << " [GiB/s] total: " << (reads+writes)/totalTime*bufSize/GIBI << " GiB/s";
   std::cout << " reads: " << reads/totalTime*bufSize/GIBI << " write: " << writes/totalTime *bufSize/GIBI;
   std::cout << " max read: " << maxRead/1e6 << "M" << std::endl;


   std::this_thread::sleep_for(std::chrono::seconds(1));
   std::cout << "fin" << std::endl;

   return 0;
}
