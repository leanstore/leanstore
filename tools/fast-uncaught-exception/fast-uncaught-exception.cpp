#include "fast-uncaught-exception.hpp"

#include <exception>

int uncaughtExceptionWrapper()
{
   //   return std::uncaught_exceptions();
   if (fastUncaughtException) {
      return fastUncaughtException();
   } else {
      return std::uncaught_exceptions();
   }
}