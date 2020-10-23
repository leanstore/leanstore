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
class CRManager
{
   class TLSHandler
   {
     public:
      Partition* p = nullptr;
      TLSHandler() { p = CRManager::registerThread(); }
      ~TLSHandler() { CRManager::removeThread(p); }
   };

  private:
   friend class TLSHandler;
   static Partition* registerThread();

  public:
   static thread_local TLSHandler tls_handler;
   static std::mutex mutex;
   static std::set<Partition*> all_threads;
   static u64 partitions_counter;
   // -------------------------------------------------------------------------------------
   CRManager();
   ~CRManager();
   // -------------------------------------------------------------------------------------
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
