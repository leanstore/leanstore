// -------------------------------------------------------------------------------------
#include "ConnectedIoChannel.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include "Exceptions.hpp"
#include "TaskExecutor.hpp"
#include "leanstore/io/IoOptions.hpp"
#include "leanstore/io/IoRequest.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
namespace mean
{
/*
// -------------------------------------------------------------------------------------
bool LocalIoChannel::push(IoRequestType type, char* data, s64 addr, u64 len, u64 user_data, u64 user_data2, IoCallbackFunction cb, bool write_back =
false) {
}
// -------------------------------------------------------------------------------------
int LocalIoChannel::submit() {
}
// -------------------------------------------------------------------------------------
int LocalIoChannel::poll() {
}
*/
///*
RemoteIoChannel::RemoteIoChannel(IoOptions ioOptions)
   : IoChannel(1), submit_ring(ioOptions.iodepth), completion_ring(ioOptions.iodepth)
{
   stack = std::make_unique<RequestStack<IoBaseRequest>>(ioOptions.iodepth);
}
// -------------------------------------------------------------------------------------
void RemoteIoChannel::_push(const IoBaseRequest& req)
{
   //int selfId = localMessageHandler.getSelfId();
   IoBaseRequest* localRequest;
   if (!stack->moveFreeToSubmitStack(localRequest)) {
      throw "no";
   }
   if (!stack->popFromSubmitStack(localRequest)) {
      throw "no";
   }
   localRequest->type = req.type;
   localRequest->data = req.data;
   localRequest->addr = req.addr;
   localRequest->len = req.len;
   localRequest->user = req.user;
   localRequest->write_back = req.write_back;
   localRequest->innerCallback.callback = [](IoBaseRequest* remote_request) {
      auto cring = reinterpret_cast<leanstore::utils::RingBufferSPSC<IoBaseRequest*>*>(remote_request->user.user_data.val.ptr); // ... is now user user_date
      IoBaseRequest* local_request  = reinterpret_cast<IoBaseRequest*>(remote_request->user.user_data2.val.ptr); // fells like a time machine
      cring->push_back(local_request);
   };
   localRequest->innerCallback.user_data.val.ptr = &completion_ring; // what has been set as innerCallback user_data ..
   localRequest->innerCallback.user_data2.val.ptr = localRequest;
   // -------------------------------------------------------------------------------------
   submit_ring.push_back(localRequest);
   pushed++;
}
bool RemoteIoChannel::readStackFull() {
   return stack->full();
}
bool RemoteIoChannel::writeStackFull() {
   return stack->free < stack->max_entries*0.5;
}
// -------------------------------------------------------------------------------------
int RemoteIoChannel::submitable() {
   return 0;
}
int RemoteIoChannel::_submit()
{
   return 0;
}
int RemoteIoChannel::_poll(int)
{
   const u64 c = 0;
   IoBaseRequest* req;
   while (completion_ring.try_pop(req)) {
      /*DEBUG_BLOCK()*/ { counters.handleCompletedReq(*req); }
      completed++;
      req->user.callback(req);
      stack->returnToFreeList(req);
   }
   return c;
}
// -------------------------------------------------------------------------------------
void RemoteIoChannel::_printSpecializedCounters(std::ostream& ss)
{
   ss << "remote[" << "]: "
      << " o: " << pushed << "  c: " << completed;
}
// -------------------------------------------------------------------------------------
//*/
}  // namespace mean
// -------------------------------------------------------------------------------------
