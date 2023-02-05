
#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "Worker.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <functional>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
   class GroupCommiter {
      void groupCommiter();
   };
}
}
