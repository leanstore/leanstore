#pragma once

#include <sys/resource.h>
#include <cstdint>

struct FreeDelete {
  void operator()(void *x);
};

auto RoundUp(uint64_t align, uint64_t num_to_round) -> uint64_t;
void SetStackSize(uint64_t stack_size_in_mb);
