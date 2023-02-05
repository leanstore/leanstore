#pragma once

#include "Time.hpp"
#include "Units.hpp"
#include "leanstore/concurrency/Mean.hpp"
#include "leanstore/io/IoInterface.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

#include <bits/stdint-uintn.h>
#include <cstdio>
#include <csignal>
#include <cstdlib>
#include <memory>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <cstring>
#include <cinttypes>
#include <sys/stat.h>
#include <sys/time.h>
#include <x86intrin.h>
#include <iostream>
#include <fstream>
#include <cerrno>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <iomanip>
#include <cassert>
#include <chrono>
#include <sstream>
#include <mutex>
#include <random>

namespace mean {

struct JobOptions {
   std::string name;

   // file
   std::string filename;
   int fd = 0;
   uint64_t filesize = 10*1024ull*1024*1024;
   uint64_t offset = 0;

   // io options
   int iodepth = 1;
   int iodepth_batch_complete_min = 0;
   int iodepth_batch_complete_max = 0;
   int bs = 4096;
   int io_alignment = 0;
   int64_t io_size = filesize; // neg value means no restrictions
   float writePercent = 0; // 0.0 - 1.0
   float requestsPerSecond = 0;
   int threads = 1;
   bool printEverySecond = false;

   int fdatasync = 0;

   enum class IoPattern {
      Sequential,
      Random
   };
   IoPattern ioPattern = IoPattern::Random;

   bool disableChecks = false;

   // Stats options
   bool enableIoTracing = false;

   bool enableLatenyTracking = false;

   //JobOptions(std::string name, std::string filename, int fd, uint64_t filesize, int iodepth, int iodepth_batch_complete_min, int iodepth_batch_complete_max, int bs, int64_t io_size, float writePercent);

   // Bytes/Second
   void setRate(float bytesPerSecond) {
      assert(bs > 0);
      requestsPerSecond = bytesPerSecond / bs;
   }

   long requestDelayNs() const {
      return requestsPerSecond > 0 ? 1/(float)requestsPerSecond * 1e9 : 0;
   }

   uint64_t totalBlocks() const {
      return filesize / bs;
   }
   uint64_t offsetBlocks() const {
      return offset / bs;
   }
   uint64_t totalMinusOffsetBlocks() const {
      return (filesize - offset) / bs;
   }

   int64_t io_operations() const {
      if (io_size < 0) {
         return -1;
      }
      return io_size / bs;
   }

   std::string print() const {
      std::ostringstream ss;
      ss << "filesize: " << filesize/(float)GIBI << " GB";
      ss << ", io_size: " << io_size/(float)GIBI << " GB";
      ss << ", bs: " << bs;
      ss << ", io_align: " << io_alignment;
      ss << ", totalBlocks: " << totalBlocks();
      ss << ", writePercent: " << writePercent;
      ss << ", iodepth: " << iodepth;
      ss << ", iodepth_batch_complete_min: " << iodepth_batch_complete_min;
      ss << ", iodepth_batch_complete_max: " << iodepth_batch_complete_max;
      ss << ", fdatasync: " << fdatasync;
      ss << std::endl;
      return ss.str();
   }
};

struct GlobalOptions : JobOptions {
   float runtime = 0;
   bool init = false;
   std::string statsFile;
   std::string ioengine;

   std::string print() const {
      std::ostringstream ss;
      ss << "runtime: " << runtime << "s";
      ss << std::endl;
      return ss.str();
   }
};

struct JobStats {
   const u64 bs;

   float time; // s
   long readTotalTime = 0; // us
   long readHghPrioTotalTime = 0; // High priority reads not used
   long writeTotalTime = 0; // us
   long fdatasyncTotalTime = 0; // us

   unsigned long reads = 0;
   unsigned long readsHighPrio = 0;
   unsigned long writes = 0;
   unsigned long fdatasyncs = 0;;

   int failedReadRequest = 0;

   std::vector<int> iopsPerSecond; //10k seconds
   std::vector<int> readsPerSecond; //10k seconds
   std::vector<int> writesPerSecond; //10k seconds
   static const int maxSeconds = 24*60*60; // 1 day in seconds
   std::atomic<int> seconds = 0;

   static const int histBuckets = 100000;
   static const int histFrom = 0;
   static const int histTo = 99999;
   Hist<int, int> readHist{histBuckets, histFrom, histTo};
   Hist<int, int> readHpHist{histBuckets, histFrom, histTo};
   Hist<int, int> writeHist{histBuckets, histFrom, histTo};
   Hist<int, int> fdatasyncHist{histBuckets, histFrom, histTo};

   Hist<int, int> readHistEverySecond{histBuckets, histFrom, histTo};
   Hist<int, int> writeHistEverySecond{histBuckets, histFrom, histTo};
   Hist<int, int> fdatasyncHistEverySecond{histBuckets, histFrom, histTo};

   void printStats(std::ostream& ss) {
      //ss <<  "time: " << time << std::endl;
      ss << "read: " << (float)reads*bs/time/GIBI << " GB/s " << reads/time << " iops ";
      ss << "50p: " << readHist.getPercentile(50) << " 99p: " << readHist.getPercentile(99) << " 99p9: " << readHist.getPercentile(99.9) << " 99p99: " << readHist.getPercentile(99.99) << std::endl;
      ss << "write: " << (float)writes*bs/time/GIBI << " GB/s " << writes/time << " iops";
      ss << "50p: " << writeHist.getPercentile(50) << " 99p: " << writeHist.getPercentile(99) << " 99p9: " << writeHist.getPercentile(99.9) << " 99p99: " << writeHist.getPercentile(99.99) << std::endl;
   }
   
   struct IoStat {
      int threadId;
      int reqId;
      TimePoint begin;
      TimePoint submit;
      TimePoint end;
      u64 addr;
      u32 len;
      IoRequestType type;

      static void dumpIoStatHeader(std::ostream& out, std::string prefix) {
         out << prefix << "threadid,reqId,type, begin, submit, end, addr, len";
      }
      void dumpIoStat(std::string prefix, std::ostream& out) {
         out << prefix << threadId << "," << reqId << "," << (int)type << "," << nanoFromTimePoint(begin) << "," << nanoFromTimePoint(submit) << "," << nanoFromTimePoint(end) << ","  << addr << "," << len;
      }
   };
   std::vector<IoStat> ioTrace;
   bool ioTracing = false;
   uint64_t evaluatedtIos = 0;

   JobStats(u64 bs) : bs(bs), iopsPerSecond(maxSeconds), readsPerSecond(maxSeconds), writesPerSecond(maxSeconds) {
      assert(iopsPerSecond.size() == maxSeconds);
   }
   JobStats(const JobStats&) = delete;
   JobStats& operator=(const JobStats&) = delete;

   void setIoTracing(bool enable) {
      ioTracing = enable;
      if (enable) {
         ioTrace.resize(10000000);
      }
   }

   static void dumpIoTreaceHeader(std::ostream& out, std::string prefix) {
      IoStat::dumpIoStatHeader(out, prefix); out << std::endl;
   }
   void dumpIoTrace(std::ostream& out, std::string prefix = "") {
      for (uint64_t i = 0; i < evaluatedtIos; i++) {
         ioTrace[i].dumpIoStat(prefix, out); 
         out << std::endl;
      }
   }

};

class RequestGeneratorBase {
public:
   std::string name;
   int genId;
   const JobOptions options;
   JobStats stats;
   std::random_device rd;
   std::mt19937_64 mersene{rd()};
   std::uniform_int_distribution<uint64_t> dist;

   RequestGeneratorBase(std::string name, JobOptions options, int genId) : name(name), genId(genId), options(options), stats(options.bs), dist(0, options.totalMinusOffsetBlocks()){
      stats.setIoTracing(options.enableIoTracing);
      //randgenId = readTSC() ^ genId ^ (std::hash<std::thread::id>{}(std::this_thread::get_id()) << 8 );
   }
   virtual ~RequestGeneratorBase() { }
};

class RequestGenerator : public RequestGeneratorBase {
   
   // data frames
   std::unique_ptr<char*[]> readData;
   std::unique_ptr<char*[]> writeData;
   char* rd;
   char* wd;

   uint64_t lastFsync = 0;
   uint64_t preparedWrites = 0;
   uint64_t preparedReads = 0;
   const int64_t totalBlocks = options.totalBlocks();
   const int64_t ops = options.io_operations();

   std::atomic<bool> keep_running = true;

   IoChannel& ioChannel;

   std::vector<u64> doneIds;
   int doneIdsCount = 0;

   unsigned long bss = options.totalMinusOffsetBlocks() / options.threads;
   std::uniform_int_distribution<unsigned long> rbs_dist{0, bss};
   unsigned long max_blocks = options.totalMinusOffsetBlocks();
   leanstore::utils::RandomGenerator random_generator{};
public:
   RequestGenerator(const RequestGenerator& other) = delete;
   RequestGenerator(RequestGenerator&& other) = delete;
   RequestGenerator& operator=(const RequestGenerator&) = delete;
   RequestGenerator& operator=(RequestGenerator&& other) = delete; 

   RequestGenerator(std::string name, JobOptions& options, IoChannel& ioChannel, int genId) : RequestGeneratorBase(name, options, genId), ioChannel(ioChannel), doneIds(options.iodepth) {
      readData = std::make_unique<char*[]>(options.iodepth);
      writeData = std::make_unique<char*[]>(options.iodepth);

      int align = 0;
      //rd = (char*)IoInterface::allocIoMemoryChecked(((options.iodepth + align)*options.bs) + 512, 512) + 512;
      rd = (char*)IoInterface::allocIoMemoryChecked(((options.iodepth + align)*options.bs), 512);
      //std::cout << "rd align 4096: " << (u64)((u64)rd % 4096) << " 512: " <<  (u64)((u64)rd % 512) << std::endl;
      //wd = (char*)IoInterface::allocIoMemoryChecked(((options.iodepth + align)*options.bs) + 512, 512);
      // WELL, seems like it is slower when not aligned to 4K
      wd = (char*)IoInterface::allocIoMemoryChecked(((options.iodepth + align)*options.bs), 512);
      for (int i = 0; i < options.iodepth; i++) {
         readData[i] = rd + ((align + options.bs)*i) + 0; //(char*) IoInterface::allocIoMemoryChecked(options.bs, 1);
         writeData[i] = wd + ((align + options.bs)*i) + 0;// (char*) IoInterface::allocIoMemoryChecked(options.bs, 1);
         memset(writeData[i], 'B', options.bs);
         memset(readData[i], 'A', options.bs);
      }
   }

   ~RequestGenerator() {
      IoInterface::freeIoMemory(rd);
      IoInterface::freeIoMemory(wd);
      /*
      for (int i = 0; i < options.iodepth; i++) {
         //IoInterface::freeIoMemory(readData[i]);
         //IoInterface::freeIoMemory(writeData[i]);
      }
      */
   }

   /* 
    * Checks first and last 4kB mark.
    * Independten of len, the marks are set every 4KB, and only the first and last of the whole len are checked.
    */
   static constexpr uint64_t CHECK_4KB = 4096;
   static bool checkBuffer(char* buf, uint64_t offset, uint64_t len) {
      // buf [ 4kb_0, 4kb_1, ..., 4kb_n ]
      // check 4kb_0[0] and 4kb_n[(4096)/sizeof(u64)-1]
      const auto* bb =  (uint64_t*)buf;
      bool ok = true; 
      ok &= bb[0] == offset / CHECK_4KB;
      ok &= bb[len / sizeof(uint64_t) - 1] == (offset + len) / CHECK_4KB -1;
      ok &= bb[1] == 5;
      // std::cout << "chk: " << bb[0] << std::endl;
      return ok;
   }

   static void checkBufferThrow(char* buf, uint64_t offset, uint64_t len) {
      if (!checkBuffer(buf, offset, len)) {
         throw std::logic_error("data check failed. Try force initialization with INIT=1");
      }
   }

   /*
    * see checkBuffer
    */
   static void writeBufferChecks(char* buf, uint64_t offset, uint64_t len) {
      assert(len % CHECK_4KB == 0);
      uint64_t pages4k = len / CHECK_4KB;
      for (uint64_t i = 0; i < pages4k; i++) {
         const uint64_t bufferOffset = i*CHECK_4KB;
         uint64_t* bb =  (uint64_t*)(buf + bufferOffset);
         bb[0] = (offset + bufferOffset) / CHECK_4KB;
         bb[1] = 5;
         bb[CHECK_4KB / sizeof(uint64_t) - 1] = (offset + bufferOffset) / CHECK_4KB;
         checkBufferThrow((char*)bb, offset+bufferOffset, CHECK_4KB);
      }
   }

   static void resetBufferChecks(char* buf, uint64_t len) {
      auto* bb =  (uint64_t*)buf;
      bb[0] = -1;
      bb[len / sizeof(uint64_t) - 1] = -2;
   }

   long __attribute__((optimize("O0"))) busyDelay(long unsigned duration) {
      auto start = getTimePoint();
      long sum = 0;
      do {
         const auto to = random_generator.getRandU64(500, 1000);
         for (u64 i = 0; i < to; i++) {
            sum += i;
         }
      } while (timePointDifference(getTimePoint(), start) < duration);
      return sum;
   }

   void stopIo() {
      keep_running = false;
   }

   int runIo() {
      // setup I/O control block
      uint64_t countGets = 0;
      //uint64_t lastOps = 0;
      //uint64_t lastReads = 0;
      //uint64_t lastWrites = 0;
      //uint64_t lastFdatasync = 0;

      //std::cout << options.name << " ready: ops:" <<  ops << " bs: " << options.bs << std::endl;

      getSeconds();
      auto start = getSeconds();

      int64_t completed = 0;
      long polled = 0;
      long submitted = 0;

      mean::UserIoCallback cb;
      cb.user_data.val.ptr = this;
      cb.user_data2.val.u = options.disableChecks;
      cb.callback = [](mean::IoBaseRequest* req) {
         //IoCallbackFunction callback = [this, &completed, &submitted, &doneIds, &doneIdsCount, dis = options.disableChecks](const IoBaseRequest* req, uintptr_t, uintptr_t) { 
         if (!req->user.user_data2.val.u) {
            checkBufferThrow(req->data, req->addr, req->len);
         }
         auto this_ptr = req->user.user_data.as<RequestGenerator*>();
         this_ptr->evaluateIocb(*req);
         this_ptr->doneIds[this_ptr->doneIdsCount++] = (req->id);
      };

      for (int i = 0; i < options.iodepth; i++) {
         // prepare iocb for next submit
         IoBaseRequest req;
         req.id = i;
         req.user = cb;
         prepareRequest(req);
         ioChannel.push(req);
         submitted++;
      }
      ioChannel.submit();
      
      long lastOps = 0;
      long lastReads = 0;
      long lastWrites = 0;
      long lastFdatasync = 0;

      const int waitBatch = 1;

      auto lapTime = getSeconds();
      volatile long busy = 0;
      while ((ops <= 0 || completed < ops) && keep_running) {
         int gotEvents = 0;
         do {
            int got = 0;
            do {
                got += ioChannel.poll();//options.iodepth_batch_complete_min, options.iodepth_batch_complete_max
            } while (got < waitBatch);
            gotEvents = got;
            if (gotEvents < 0) {
               throw std::logic_error("could not get events. gotEvents is " + std::to_string(gotEvents));
            }
            assert(doneIdsCount == gotEvents);
            int i = 0;
            completed += gotEvents;
            for (; i < gotEvents && ((ops <= 0 || completed < ops) && keep_running); i++) {
               IoBaseRequest reqCpy;
               reqCpy.id = doneIds[i];
               reqCpy.user = cb;
               prepareRequest(reqCpy);
               ioChannel.push(reqCpy);
               //std::cout << name << " " << reqCpy.addr << std::endl;
               //busy = busyDelay(2200);
            }
            if (gotEvents > 0) {
               ioChannel.submit();
            }
            submitted += i;
            doneIdsCount = 0;


         } while (gotEvents <= 0);
         polled += gotEvents;
         countGets++;

        
         auto cycleEnd = getSeconds();
         if (cycleEnd - lapTime >= 1) {
            if (ops > 0) {
               std::cout << completed*100/ops << " " << std::flush;
            }
            lapTime = cycleEnd;
            ///*
            stats.iopsPerSecond[stats.seconds] = completed - lastOps;
            stats.readsPerSecond[stats.seconds] = stats.reads - lastReads;
            stats.writesPerSecond[stats.seconds]= stats.writes - lastWrites;
            stats.seconds++;
            if (stats.seconds >= JobStats::maxSeconds) {
               throw std::logic_error("over max seconds");
            }
            //if (seconds % 10 == 0) {
            /*auto completedPercent = ops > 0 ? (int)((completed/(float)ops)*100) : 0;
            cout << options.name << ": " << completedPercent << "% read: " 
            << (float)stats.readsPerSecond[stats.seconds-1]*options.bs/MEBI << " MB/s write: " 
            << (float)stats.writesPerSecond[stats.seconds-1]*options.bs/MEBI << " MB/s " <<  endl;
            //}
            */

            if (options.printEverySecond) {
               std::stringstream ss;
               ss << options.name << "," << stats.seconds << ",r," 
                  << stats.readsPerSecond[stats.seconds-1] << ",w,"
                  << stats.writesPerSecond[stats.seconds-1] << ",s,"
                  << stats.fdatasyncs - lastFdatasync;
               ss << ",r,";
               stats.readHistEverySecond.writePercentiles(ss);
               ss << ",w,";
               stats.writeHistEverySecond.writePercentiles(ss);
               ss << ",s,";
               stats.fdatasyncHistEverySecond.writePercentiles(ss);
               std::cout << ss.str() << std::endl;;
               //options.statsWriter->write(ss.str());
               std::cout << ss.str();
            }

            // TODO rather expensive.
            stats.readHistEverySecond.resetData();
            stats.writeHistEverySecond.resetData();
            stats.fdatasyncHistEverySecond.resetData();

            lastOps = completed;
            lastReads = stats.reads;
            lastWrites = stats.writes;
            lastFdatasync = stats.fdatasyncs;
            //*/
         }
      }
      std::cout << std::endl;
      stats.time = (getSeconds() - start);

      // done. get the remaining events.
      while (polled < submitted) {
         ioChannel.submit();
         polled += ioChannel.poll();
      }
      ioChannel.printCounters(std::cout);

      // 
      return 0;
   }

   uint64_t patternGenerator(uint64_t prep, std::mt19937_64& mersene, std::uniform_int_distribution<uint64_t>& dist) const {
      assert(options.offset % options.bs == 0);
      uint64_t block = 0;
      if (options.ioPattern == JobOptions::IoPattern::Random) {
         block = random_generator.getRandU64(0, max_blocks); //dist(mersene);
      } else {
         block = prep;
      }
      if (options.io_alignment <= 0) {
         return  (block % options.totalMinusOffsetBlocks()) * options.bs + options.offset ;
      } else {
         u64 addr = (block % ((options.filesize - options.offset) / options.io_alignment)) * options.io_alignment + options.offset ;
         return addr;
      }
   }

   void prepareRequest(IoBaseRequest& req) {
      if (options.fdatasync > 0 && preparedWrites - lastFsync == (uint64_t)options.fdatasync) {
         //c->aio_lio_opcode = IO_CMD_FDSYNC;
         std::logic_error("not yet implemented");
         lastFsync = preparedWrites;
      } else {
         req.len = options.bs;
         assert(std::mt19937_64::min() == 0);
         if (options.writePercent > 0 && (float)mersene() / std::mt19937_64::max() < options.writePercent) {
            // write
            req.addr = patternGenerator(preparedWrites, mersene, dist); 
            //req.write_back = true;
            req.type = IoRequestType::Write;
            req.data =  writeData[req.id];
            writeBufferChecks(req.data, req.addr, req.len);
            preparedWrites++;
         } else {
            // read
            req.addr = patternGenerator(preparedReads, mersene, dist);
            ///
            /*
            unsigned long rbs = rbs_dist(mersene);
            req.addr = (rbs*options.bs/(64*1024))*64*1024*options.threads + genId*64*1024 + (rbs*options.bs % 64*1024);
            std::string bla = "threasd: " + std::to_string(options.threads) + "genId: " + std::to_string(genId) +" addr: " + std::to_string(req.addr);
            std::cout << bla << std::endl;
            //*/
            assert(req.addr <= options.filesize + options.threads*64*1024);
            assert(req.addr % 512 == 0);
            req.type = IoRequestType::Read;
            req.data = readData[req.id];
            preparedReads++;
         }
      }
   }

   uint64_t evaluateIocb(const IoBaseRequest& req) {
      uint64_t sum = 0;
      //ensure(req.device == genId);
      if (options.enableIoTracing) {
         JobStats::IoStat& ios = stats.ioTrace.at(stats.evaluatedtIos);
         ios.threadId = genId;
         ios.reqId = req.id;
         ios.begin = req.stats.push_time;
         ios.submit = req.stats.submit_time;
         ios.end = req.stats.completion_time;
         ios.type = req.type;
         ios.addr = req.addr;
         ios.len = req.len;
         stats.evaluatedtIos++;
      }
      if (!options.enableIoTracing) {
         if (req.type == IoRequestType::Read) {
            stats.reads++;
         } else if (req.type == IoRequestType::Write) { 
            stats.writes++;
         } else {
            stats.fdatasyncs++;
            raise(SIGTRAP);
         }
      } else {
         const auto thisTime = timePointDifference(getTimePoint(), req.stats.push_time) / 1000;
         if (req.type == IoRequestType::Fsync) {
            stats.fdatasyncTotalTime += thisTime;
            stats.fdatasyncHist.increaseSlot(thisTime);
            stats.fdatasyncHistEverySecond.increaseSlot(thisTime);
            stats.fdatasyncs++;
            raise(SIGTRAP);
         } else if (req.type == IoRequestType::Read) {
            sum += ((uint64_t*)req.data)[0];
            //if (c->aio_reqprio == 1) {
            // stats.readHghPrioTotalTime += thisTime;
            // stats.readHpHist.increaseSlot(thisTime);
            // stats.readsHighPrio++;
            //} else {
            stats.readTotalTime += thisTime;
            stats.readHist.increaseSlot(thisTime);
            stats.reads++;
            //}
            stats.readHistEverySecond.increaseSlot(thisTime);
            //assert(((char*)(*c).aio_buf)[0] == (char)(*c).aio_offset);
         } else { 
            stats.writeTotalTime += thisTime;
            stats.writeHist.increaseSlot(thisTime);
            stats.writeHistEverySecond.increaseSlot(thisTime);
            stats.writes++;
         }
      }
      return sum;
   }
};
};
