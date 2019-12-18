#include "DebuggingCounters.hpp"
namespace leanstore
{
thread_local DebuggingCounters::ThreadLocalCounters DebuggingCounters::thread_local_counters;
}