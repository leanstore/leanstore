#pragma once
// -------------------------------------------------------------------------------------
#include "Time.hpp"
#include "Units.hpp"
#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <chrono>
#include <memory>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
enum class IoRequestType { Undefined = 0, Write, Read, Fsync };
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
   struct {
      TimePoint push_time;
      TimePoint submit_time;
      TimePoint completion_time;
      void push() { COUNTERS_BLOCK() { push_time = getTimePoint(); } }
      void submit() { COUNTERS_BLOCK() { submit_time = getTimePoint(); } }
   } stats;
   // -------------------------------------------------------------------------------------
   IoBaseRequest() { id = 0xAA00AA00AA00AA00; }
   IoBaseRequest(IoRequestType type, char* data, u64 addr, u64 len, UserIoCallback user, bool write_back = false);
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
