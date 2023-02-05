#include "IoRequest.hpp"

namespace mean
{
// -------------------------------------------------------------------------------------
IoBaseRequest::IoBaseRequest(IoRequestType type, char* data, u64 addr, u64 len, UserIoCallback user, bool write_back)
    : type(type),
      data(data),
      addr(addr),
      len(len),
      user(user),
      write_back(write_back),
      write_back_buffer(nullptr)
{
}

char* IoBaseRequest::buffer() {
   return write_back ? write_back_buffer : data;
}
// -------------------------------------------------------------------------------------
void IoBaseRequest::copyFields(const IoBaseRequest& usr)
{
   id = usr.id;
   type = usr.type;
   data = usr.data;
   addr = usr.addr;
   len = usr.len;
   user = usr.user;
   write_back = usr.write_back;
}
// -------------------------------------------------------------------------------------
void IoBaseRequest::print(std::ostream& ss) const
{
   ss << "type: " << (int)type << " off: " << addr << " len: " << len << " buf: 0x" << std::hex << (u64)data << std::dec;
   ss << " wb: " << write_back << " wbbuf: 0x" << std::hex << (u64)write_back_buffer << std::dec;
   ss << std::endl;
}
// -------------------------------------------------------------------------------------
}
