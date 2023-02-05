#include "IoChannel.hpp"
// -------------------------------------------------------------------------------------
namespace mean
{
void IoChannel::push(const IoBaseRequest& req)
{
   _push(req);
   COUNTERS_BLOCK() { counters.handlePush(); }
}
int IoChannel::submit()
{
   int submitted = _submit();
   COUNTERS_BLOCK() { counters.handleSubmit(submitted); }
   return submitted;
}
int IoChannel::poll(int min)
{
   int done = _poll(min);
   COUNTERS_BLOCK() { counters.handlePoll(done); }
   return done;
}
// -------------------------------------------------------------------------------------
void IoChannel::push(IoRequestType type, char* data, s64 addr, u64 len, UserIoCallback cb, bool write_back)
{
   IoBaseRequest req(type, data, addr, len, cb, write_back);
   _push(req);
   COUNTERS_BLOCK() { counters.handlePush(); }
}
void IoChannel::pushWrite(char* data, s64 addr, u64 len, UserIoCallback cb, bool write_back)
{
   push(IoRequestType::Write, data, addr, len, cb, write_back);
}
void IoChannel::pushRead(char* data, s64 addr, u64 len, UserIoCallback cb, bool write_back)
{
   push(IoRequestType::Read, data, addr, len, cb, write_back);
}

void IoChannel::pushBlocking(IoRequestType type, char* data, s64 addr, u64 len, bool write_back)
{
   throw std::logic_error("not implemented");
   /*
   std::atomic<int> waitDone = 0;
   push(
       type, data, addr, len, reinterpret_cast<uintptr_t>(&waitDone), 0,
       [](void*, uintptr_t donePtr, uintptr_t) {
          auto& waitDone = *reinterpret_cast<std::atomic<int>*>(donePtr);
          waitDone = 1;
       },
       write_back);
   submit();
   while (waitDone == 0) {
      poll();
   }
   */
}
// -------------------------------------------------------------------------------------
void IoChannel::printCounters(std::ostream& ss)
{
   _printSpecializedCounters(ss);
   counters.printCounters(ss);
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
