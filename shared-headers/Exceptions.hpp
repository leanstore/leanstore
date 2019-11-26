#pragma once
#include "Units.hpp"
#include <exception>
#include <string>
#include <signal.h>
// -------------------------------------------------------------------------------------
#define imply(lhs, rhs) \
    (!(lhs) || (rhs))
// -------------------------------------------------------------------------------------
#define posix_check(expr) if (!(expr)) { perror(#expr); assert(false); }
//--------------------------------------------------------------------------------------
#define Generic_Exception(name)                                                         \
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
namespace leanstore {
namespace ex {
Generic_Exception(GenericException);
Generic_Exception(EnsureFailed);
Generic_Exception(UnReachable);
Generic_Exception(TODO);
void OnEnsureFailedPrint(const std::string &func, const std::string &file, int line, const std::string &expression);
}
}
// -------------------------------------------------------------------------------------
#define UNREACHABLE() \
    throw ex::UnReachable(std::string(__FILE__) + ":" + std::string(std::to_string(__LINE__)));
// -------------------------------------------------------------------------------------
#define   ensure(e) \
    (__builtin_expect(!(e), 0) ? throw ex::EnsureFailed(std::string(__func__) + " in " + std::string( __FILE__) + "@" + std::to_string(__LINE__) + " msg: " + std::string(#e)) : (void)0)
// -------------------------------------------------------------------------------------
#define TODO() throw leanstore::ex::TODO(std::string(__FILE__) + ":" + std::string(std::to_string(__LINE__)));
// -------------------------------------------------------------------------------------
#define   explain(e) if(!(e)) { raise(SIGTRAP); };
// -------------------------------------------------------------------------------------