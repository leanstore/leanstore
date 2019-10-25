#include "BufferFrame.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore{
// -------------------------------------------------------------------------------------
BufferFrame::BufferFrame(PID pid) {
   header.pid = pid;
}
std::vector<Swip*> dummyCallback(u8* payload, SwizzlingCallbackCommand command) {
   return {};
}
}
// -------------------------------------------------------------------------------------