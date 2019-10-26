#pragma once
#include "BufferManager.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore{
class SmartPointer {
public:
   BufferFrame &swip_holder;
   Swip &swip;
   SharedLock lock;
   BufferFrame *bf;
   SmartPointer(BufferFrame &swip_holder, Swip &swip);
};
}
