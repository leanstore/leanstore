#include "benchmark/benchmark.h"
#include "share_headers/logger.h"

#include <cmath>
#include <limits>

#define NO_OPERATION 100

#define SIZEBIT(datatype) (sizeof(datatype) * CHAR_BIT)

#define DUMMY(i, datatype) ((1 << ((i) % SIZEBIT(datatype))) - 1)

#define SET_RANGE_RANDOM(length, datatype)                                                                      \
  ({                                                                                                            \
    uint64_t start_pos = rand() % (length);                                                                     \
    auto size          = rand() % (length - start_pos);                                                         \
    for (auto i = start_pos; i < start_pos + size; i++) {                                                       \
      auto next_pos = std::min(i + SIZEBIT(datatype) - i % SIZEBIT(datatype), start_pos + size);                \
      auto val =                                                                                                \
        (next_pos % SIZEBIT(datatype) == 0) ? std::numeric_limits<datatype>::max() : DUMMY(next_pos, datatype); \
      if (i % SIZEBIT(datatype) != 0) { val -= DUMMY(i, datatype); }                                            \
      if (rand() % 2 == 0) {                                                                                    \
        map[i / SIZEBIT(datatype)] |= val;                                                                      \
      } else {                                                                                                  \
        map[i / SIZEBIT(datatype)] &= !(val);                                                                   \
      }                                                                                                         \
      i = next_pos - 1;                                                                                         \
    }                                                                                                           \
  })

static void BM_Int8(benchmark::State &state) {
  bool map[state.range(0) / SIZEBIT(uint8_t)];

  for (auto _ : state) {
    for (int i = 0; i < NO_OPERATION; i++) { SET_RANGE_RANDOM(state.range(0), uint8_t); }
  }
}

static void BM_Int32(benchmark::State &state) {
  bool map[state.range(0) / SIZEBIT(uint32_t)];

  for (auto _ : state) {
    for (int i = 0; i < NO_OPERATION; i++) { SET_RANGE_RANDOM(state.range(0), uint32_t); }
  }
}

static void BM_Int64(benchmark::State &state) {
  bool map[state.range(0) / SIZEBIT(uint64_t)];

  for (auto _ : state) {
    for (int i = 0; i < NO_OPERATION; i++) { SET_RANGE_RANDOM(state.range(0), uint64_t); }
  }
}

static void BM_Int128(benchmark::State &state) {
  bool map[state.range(0) / SIZEBIT(__int128_t)];

  for (auto _ : state) {
    for (int i = 0; i < NO_OPERATION; i++) { SET_RANGE_RANDOM(state.range(0), __int128_t); }
  }
}

BENCHMARK(BM_Int8)->Arg(256)->Arg(1024)->Arg(1 << 16);
BENCHMARK(BM_Int32)->Arg(256)->Arg(1024)->Arg(1 << 16);
BENCHMARK(BM_Int64)->Arg(256)->Arg(1024)->Arg(1 << 16);
BENCHMARK(BM_Int128)->Arg(256)->Arg(1024)->Arg(1 << 16);

BENCHMARK_MAIN();