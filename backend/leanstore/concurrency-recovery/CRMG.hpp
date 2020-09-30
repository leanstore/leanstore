#pragma once
#include "Exceptions.hpp"
#include "Partition.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <mutex>
#include <set>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// Static class
class CRMG
{
  struct TLSHandler {
    Partition* p;
    TLSHandler() { p = CRMG::registerThread(); }
    ~TLSHandler() { CRMG::removeThread(p); }
  };

 public:
  static thread_local TLSHandler tls_handler;
  static std::mutex mutex;
  static std::set<Partition*> all_threads;
  static u64 partitions_counter;
  // -------------------------------------------------------------------------------------
  CRMG();
  ~CRMG();
  // -------------------------------------------------------------------------------------
  static Partition* registerThread();
  static void removeThread(Partition*);
  // -------------------------------------------------------------------------------------
  inline static Partition& my()
  {
    assert(tls_handler.p != nullptr);
    return *tls_handler.p;
  }
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
