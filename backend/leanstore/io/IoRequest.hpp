#pragma once
// -------------------------------------------------------------------------------------
#include "Time.hpp"
#include "Units.hpp"
#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <memory>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
enum class IoRequestType { Undefined = 0, Write, Read, Fsync, ZnsAppend, Free };
// -------------------------------------------------------------------------------------
class IoBaseRequest;
using UserIoCallbackFunPtr = void (*)(IoBaseRequest* );
struct PtrInt {
   union {
      void* ptr;
      s64 s;
      u64 u;
   } val;
   template <typename CAST_TO>
   CAST_TO as() {
      return reinterpret_cast<CAST_TO>(val.ptr);
   };
};
struct UserIoCallback {
   UserIoCallbackFunPtr callback = nullptr;
   PtrInt user_data;
   PtrInt user_data2;
   PtrInt user_data3;
   UserIoCallback() { }
   UserIoCallback(UserIoCallbackFunPtr cb) { callback = cb; }
};
// -------------------------------------------------------------------------------------
class IoBaseRequest
{
  public:
   u64 id = 1000001;
   // -------------------------------------------------------------------------------------
   IoRequestType type = IoRequestType::Undefined;
   // -------------------------------------------------------------------------------------
   char* data = nullptr;
   u64 addr = -1;
   u64 len = -1;
   u64 out_of_place_addr = -1;
   UserIoCallback user;
   // -------------------------------------------------------------------------------------
   int device = 0;
   u64 offset = 0;
   // -------------------------------------------------------------------------------------
   UserIoCallback innerCallback;
   u64 innerData = -1;
   // -------------------------------------------------------------------------------------
   bool write_back = false;
   char* write_back_buffer = nullptr;
   // -------------------------------------------------------------------------------------
   uint8_t hint = -1;
   // -------------------------------------------------------------------------------------
   // if set the request object won't be returned to the I/O interface request pool.
   // This can be used to reuse the request inside a request callback, i.e. to immediately
   // re-submit it. The flag must be set before the end of the user callback.
   bool reuse_request = false;
   // -------------------------------------------------------------------------------------
   struct {
      uint64_t push_time;
      uint64_t submit_time;
      uint64_t completion_time;
   } stats;
   // -------------------------------------------------------------------------------------
   IoBaseRequest() { id = 0xAA00AA00AA00AA00; }
   IoBaseRequest(IoRequestType type, char* data, u64 addr, u64 len, UserIoCallback user, bool write_back = false);
   //
   void setupWrite(char* data, s64 addr, u64 len, UserIoCallback cb, bool write_back = false) { this->type = IoRequestType::Write; this->data = data; this->addr = addr; this->len = len; this->user = cb; this->write_back = write_back; this->reuse_request = false; }
   void setupRead(char* data, s64 addr, u64 len, UserIoCallback cb)                           { this->type = IoRequestType::Read;  this->data = data; this->addr = addr; this->len = len; this->user = cb;                                this->reuse_request = false; }
   // -------------------------------------------------------------------------------------
   char* buffer();
   // -------------------------------------------------------------------------------------
   void copyFields(const IoBaseRequest& usr);
   void print(std::ostream& ss) const;
};
template <typename TImplRequest>
struct RaidRequest {
   TImplRequest impl;
   IoBaseRequest base;
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
