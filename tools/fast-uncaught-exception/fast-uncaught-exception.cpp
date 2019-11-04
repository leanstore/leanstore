#include <exception>
#include "fast-uncaught-exception.hpp"

int uncaughtExceptionWrapper() {
//   return std::uncaught_exceptions();
   if(fastUncaughtException) {
      return fastUncaughtException();
   } else {
      return std::uncaught_exceptions();
   }
}