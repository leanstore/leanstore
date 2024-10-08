#include "benchmark/utils/misc.h"

#include <cassert>
#include <cstdlib>
#include <stdexcept>

void FreeDelete::operator()(void *x) { free(x); }

auto RoundUp(uint64_t align, uint64_t num_to_round) -> uint64_t {
  assert(align && ((align & (align - 1)) == 0));
  return (num_to_round + align - 1) & -align;
}

void SetStackSize(uint64_t stack_size_in_mb) {
  const rlim_t k_stack_size = stack_size_in_mb * 1024 * 1024;  // min stack size = 16 MB
  struct rlimit rl;
  int result;

  result = getrlimit(RLIMIT_STACK, &rl);
  if (result == 0) {
    if (rl.rlim_cur < k_stack_size) {
      rl.rlim_cur = k_stack_size;
      result      = setrlimit(RLIMIT_STACK, &rl);
      if (result != 0) { throw std::runtime_error("Err: setrlimit returned result =: " + std::to_string(result)); }
    }
  }
}
