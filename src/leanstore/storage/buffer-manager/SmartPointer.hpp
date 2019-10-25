#pragma once
#include "BufferManager.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore{
class SmartPointer {
public:
   Swip &swip;
   SharedLock lock;
   BufferFrame *bf;
   SmartPointer(Swip &swip);
};
}
