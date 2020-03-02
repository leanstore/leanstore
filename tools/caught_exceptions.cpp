#include <atomic>
#include <exception>
#include <iostream>
#include "PerfEvent.hpp"
#include "fast-uncaught-exception/fast-uncaught-exception.hpp"

using namespace std;

int (*exc_func)() = nullptr;

struct Guard {
  uint64_t local_version;
  atomic<uint64_t>& ref_lock;
  Guard(atomic<uint64_t>& lock) : ref_lock(lock) { local_version = lock.load(); }
  ~Guard()
  {
    if (exc_func() == 0) {
      if (ref_lock.load() != local_version) {
        throw exception();
      }
    }
  }
};
int main(int argc, char** argv)
{
  if (fastUncaughtException) {
    exc_func = fastUncaughtException;
  } else {
    exc_func = std::uncaught_exceptions;
  }
  uint32_t n = getenv("N") ? atoi(getenv("N")) : 10e4;

  atomic<uint64_t> locks[4];
  PerfEvent e;
  PerfEventBlock b(e, n);
  for (auto i = 0; i < n; i++) {
    {
      Guard g1(locks[0]);
      Guard g2(locks[1]);
      Guard g3(locks[2]);
      Guard g4(locks[3]);
    }
  }
  return 0;
}
