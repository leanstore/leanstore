#pragma once
#include <cassert>
#include <exception>
#include <string>
// -------------------------------------------------------------------------------------
#define UNREACHABLE() assert(false); // TODO
#define TODO() assert(false); // TODO
// -------------------------------------------------------------------------------------
#define check(expr) if (!(expr)) { perror(#expr); assert(false); }
//--------------------------------------------------------------------------------------
#define GenericException(name)                                                         \
struct name : public std::exception {                                                  \
   const std::string msg;                                                              \
   explicit name()                                                                     \
      : msg(#name) { printf("Throwing exception: %s\n", #name); }                      \
   explicit name(const std::string& msg)                                               \
      : msg(msg) { printf("Throwing exception: %s(%s)\n", #name, msg.c_str()); }       \
   ~name() = default;                                                                  \
   virtual const char *what() const noexcept { return msg.c_str(); }                   \
};                                                                                     \
//--------------------------------------------------------------------------------------
GenericException(Generic_Exception);
// -------------------------------------------------------------------------------------