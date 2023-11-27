#include "storage/blob/compressor.h"

#include "benchmark/benchmark.h"
#include "share_headers/logger.h"

using BlobCompress = leanstore::storage::blob::BlobCompressor;

static void BM_FSST(benchmark::State &state) {
  BlobCompress blob;
  uint8_t data[state.range(0)]     = {0};
  std::span<const uint8_t> payload = {data, static_cast<size_t>(state.range(0))};

  for (auto _ : state) {
    blob.Reset(0);
    blob.FSSTCompressBlob(payload);
  }
  state.counters["Compressed size in bytes"] =
    benchmark::Counter(blob.size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
}

static void BM_FSST_Random(benchmark::State &state) {
  BlobCompress blob;
  uint8_t data[state.range(0)] = {0};
  for (auto idx = 0; idx < state.range(0); idx++) { data[idx] = rand() % 255; }
  std::span<const uint8_t> payload = {data, static_cast<size_t>(state.range(0))};

  for (auto _ : state) {
    blob.Reset(0);
    blob.FSSTCompressBlob(payload);
  }
  state.counters["Compressed size in bytes"] =
    benchmark::Counter(blob.size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
}

static void BM_ZSTD(benchmark::State &state) {
  BlobCompress blob;
  uint8_t data[state.range(0)]     = {0};
  std::span<const uint8_t> payload = {data, static_cast<size_t>(state.range(0))};

  for (auto _ : state) {
    blob.Reset(0);
    blob.ZSTDCompressBlob(payload);
  }
  state.counters["Compressed size in bytes"] =
    benchmark::Counter(blob.size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
}

static void BM_ZSTD_Random(benchmark::State &state) {
  BlobCompress blob;
  uint8_t data[state.range(0)] = {0};
  for (auto idx = 0; idx < state.range(0); idx++) { data[idx] = rand() % 255; }
  std::span<const uint8_t> payload = {data, static_cast<size_t>(state.range(0))};

  for (auto _ : state) {
    blob.Reset(0);
    blob.ZSTDCompressBlob(payload);
  }
  state.counters["Compressed size in bytes"] =
    benchmark::Counter(blob.size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
}

static void BM_LZ4(benchmark::State &state) {
  BlobCompress blob;
  uint8_t data[state.range(0)]     = {0};
  std::span<const uint8_t> payload = {data, static_cast<size_t>(state.range(0))};

  for (auto _ : state) {
    blob.Reset(0);
    blob.LZ4CompressBlob(payload);
  }
  state.counters["Compressed size in bytes"] =
    benchmark::Counter(blob.size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
}

static void BM_LZ4_Random(benchmark::State &state) {
  BlobCompress blob;
  uint8_t data[state.range(0)] = {0};
  for (auto idx = 0; idx < state.range(0); idx++) { data[idx] = rand() % 255; }
  std::span<const uint8_t> payload = {data, static_cast<size_t>(state.range(0))};

  for (auto _ : state) {
    blob.Reset(0);
    blob.LZ4CompressBlob(payload);
  }
  state.counters["Compressed size in bytes"] =
    benchmark::Counter(blob.size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
}

BENCHMARK(BM_FSST)->Range(64, 64 << 10);
BENCHMARK(BM_FSST_Random)->Range(64, 64 << 10);
BENCHMARK(BM_ZSTD)->Range(64, 64 << 10);
BENCHMARK(BM_ZSTD_Random)->Range(64, 64 << 10);
BENCHMARK(BM_LZ4)->Range(64, 64 << 10);
BENCHMARK(BM_LZ4_Random)->Range(64, 64 << 10);

BENCHMARK_MAIN();