#pragma once
// -------------------------------------------------------------------------------------
#include "IoRequest.hpp"
#include "IoOptions.hpp"
#include "Units.hpp"
#include "Exceptions.hpp"
#include "leanstore/utils/Hist.hpp"
#include "RequestStack.hpp"
#include "Raid.hpp"
#include "leanstore/profiling/counters/SSDCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <cstring>
#include <vector>
#include <iomanip>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
struct IoChannelCounterAggregator;
struct IoChannelCounters {
   std::atomic<u64> pushed = 0;
   std::atomic<s64> outstanding = 0;
   std::atomic<s64> outstandingRead = 0;
   std::atomic<s64> outstandingWrite = 0;
   std::atomic<u64> completed = 0;
   // -------------------------------------------------------------------------------------
   Hist<int, u64> readHist{1000, 0, 10000};
   Hist<int, u64> writeHist{1000, 0, 10000};
   // -------------------------------------------------------------------------------------
   Hist<int, u64> pollHist{1000, 0, 500};
   Hist<int, u64> outstandingHist{1000, 0, 2000};
   Hist<int, u64> outstandingHistRead{1000, 0, 2000};
   Hist<int, u64> outstandingHistWrite{1000, 0, 2000};
   // -------------------------------------------------------------------------------------
   struct DeviceCounters {
      int outstanding = 0;
      Hist<int, u64> readHist{1000, 0, 100000};
      Hist<int, u64> writeHist{1000, 0, 100000};
   };
   std::vector<DeviceCounters> device_counters;
   // -------------------------------------------------------------------------------------
   IoChannelCounters(int deviceCount) : device_counters(deviceCount) { }
   // -------------------------------------------------------------------------------------
   void reset()
   {
      // pushed = 0;
      // outstanding = 0;
      completed = 0;
      readHist.resetData();
      writeHist.resetData();
      pollHist.resetData();
      for (unsigned i = 0; i < device_counters.size(); i++) {
         device_counters[i].writeHist.resetData();
         device_counters[i].readHist.resetData();
      }
      outstandingHist.resetData();
      outstandingHistRead.resetData();
      outstandingHistWrite.resetData();
   }
   void handlePush() { pushed++; }
   void handleSubmit(int submitted)
   {
      pushed.fetch_add(-submitted);
      outstanding.fetch_add(submitted);
      outstandingHist.increaseSlot(outstanding);
   }
   void handlePoll(int polled)
   {
      outstanding.fetch_add(-polled);
      completed.fetch_add(polled);
      pollHist.increaseSlot(polled);
   }
   void handleSubmitReq(IoBaseRequest& req)
   {
      device_counters[req.device].outstanding++;
   }
   void handleCompletedReq(IoBaseRequest& req)
   {
      req.stats.completion_time = getTimePoint();
      const auto diff = timePointDifferenceUs(req.stats.completion_time, req.stats.push_time);
      device_counters[req.device].outstanding--;
      if (req.type == IoRequestType::Read) {
         readHist.increaseSlot(diff);
         device_counters[req.device].readHist.increaseSlot(diff);
         leanstore::SSDCounters::myCounters().reads[req.device]++;
         outstandingHistRead.increaseSlot(outstandingRead);
         outstandingRead--;
         if (outstandingRead < 0 ) {
            raise(SIGINT);
         }
      } else if (req.type == IoRequestType::Write) {
         writeHist.increaseSlot(diff);
         device_counters[req.device].writeHist.increaseSlot(diff);
         leanstore::SSDCounters::myCounters().writes[req.device]++;
         outstandingHistWrite.increaseSlot(outstandingWrite);
         outstandingWrite--;
      }
   }
   void printCountersHeader(std::ostream& ss) {
     ss << "pushedx,outstandingx,completed_k,read50,read99p9,write50,write99p9,poll10,poll50,poll90,out10,out50,out90";
   }
   void printCounters(std::ostream& ss)
   {
      ss << std::setprecision(3);
      ss << pushed << "," << outstanding << "," << completed / KILO << ",";
      ss << readHist.getPercentile(50) << "," << readHist.getPercentile(99.9) << ",";
      ss << writeHist.getPercentile(50) << "," << writeHist.getPercentile(99.9) << ",";
      ss << pollHist.getPercentile(10) << "," << pollHist.getPercentile(50) << "," << pollHist.getPercentile(90) << ",";
      ss << outstandingHist.getPercentile(10) << "," << outstandingHist.getPercentile(50) << "," << outstandingHist.getPercentile(90);
   }
   void updateLeanStoreCounters() {
      auto& c = leanstore::SSDCounters::myCounters();
      for (unsigned i = 0; i < device_counters.size(); i++) {
         c.read_latncy50p[i] = device_counters[i].readHist.getPercentile(50);
         c.read_latncy99p9[i] = device_counters[i].readHist.getPercentile(99.9);
         c.read_latncy_max[i] = device_counters[i].readHist.getPercentile(100);
         c.write_latncy50p[i] = device_counters[i].writeHist.getPercentile(50);
         c.write_latncy99p9[i] = device_counters[i].writeHist.getPercentile(99.9);
         c.outstandingx_max[i] = std::max(device_counters[i].outstanding, (int)c.outstandingx_max[i].load());
         c.outstandingx_min[i] = std::min(device_counters[i].outstanding, (int)c.outstandingx_min[i].load());
      }
      leanstore::PPCounters::myCounters().outstandinig_50p = outstandingHist.getPercentile(50);
      leanstore::PPCounters::myCounters().outstandinig_99p9 = outstandingHist.getPercentile(99.9);
      leanstore::PPCounters::myCounters().outstandinig_read= outstandingHistRead.getPercentile(50);
      leanstore::PPCounters::myCounters().outstandinig_write = outstandingHistWrite.getPercentile(50);
   }
};
struct IoChannelCounterAggregator {
   u64 totalPushed = 0;
   u64 totalOutstanding = 0;
   u64 totalCompleted = 0;
   int maxRead99p9 = 0;
   int maxWrite99p9 = 0;
   int count = 0;
   void aggregate(IoChannelCounters& counters)
   {
      count++;
      totalPushed += counters.pushed;
      totalOutstanding += counters.outstanding;
      totalCompleted += counters.completed;
      maxRead99p9 = std::max(maxRead99p9, (int)counters.readHist.getPercentile(99.9));
      maxWrite99p9 = std::max(maxWrite99p9, (int)counters.writeHist.getPercentile(99.9));
   }
   void print(std::ostream& ss)
   {
      ss << "ioaggr:(" << count << ")[p: " << totalPushed << " o: " << totalOutstanding << " c: " << totalCompleted / KILO << "k] ";
      ss << "hist: [read:( m99p9:" << maxRead99p9 << ")";
      ss << " write:(m99p9:" << maxWrite99p9 << ")]";
   }
};
class RemoteIoChannel;
class IoChannel
{
  public:
   // -------------------------------------------------------------------------------------
   IoChannelCounters counters;
   // -------------------------------------------------------------------------------------
   IoChannel(int devices) : counters(devices) { }
   // -------------------------------------------------------------------------------------
   virtual ~IoChannel(){ };
   // -------------------------------------------------------------------------------------
   void printCounters(std::ostream& ss);
   // -------------------------------------------------------------------------------------
   void push(const IoBaseRequest& req);
   int submit();
   int poll(int min = 0);
   // -------------------------------------------------------------------------------------
   void pushWrite(char* data, s64 addr, u64 len, UserIoCallback cb, bool write_back = false);
   void pushRead(char* data, s64 addr, u64 len, UserIoCallback cb, bool write_back = false);
   void push(IoRequestType type, char* data, s64 addr, u64 len, UserIoCallback cb, bool write_back = false);
   // -------------------------------------------------------------------------------------
   virtual void _push(const IoBaseRequest& req) = 0;
   virtual int _submit() = 0;
   virtual int _poll(int min = 0) = 0;
   virtual void _printSpecializedCounters(std::ostream& ss) = 0;
   virtual void pushBlocking(IoRequestType type, char* data, s64 addr, u64 len, bool write_back = false);
   virtual bool readStackFull() = 0;
   virtual bool writeStackFull() = 0;
   virtual void registerRemoteChannel(RemoteIoChannel* rem) = 0;
   // -------------------------------------------------------------------------------------
   virtual int submitMin() {return 1; } // minium number of ios so that they can be submitted. Else less or none of the ios that have been pushed will be pushed.
   virtual int submitable() = 0;
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
