#include "storage/blob/blob_state.h"

#include "benchmark/benchmark.h"
#include "fmt/ranges.h"
#include "gcrypt.h"
#include "openssl/evp.h"
#include "prototype/git_sha256.h"
#include "prototype/random_sha256.h"
#include "share_headers/logger.h"
#include "share_headers/picosha2.h"

#include <iostream>

#define SHA256_DIGEST_SIZE 32

/**
 * @brief All SHA-256 variants
 *
 * Noloader SHA Intrinsics - SHA2SIMD benchmark
 * https://github.com/noloader/SHA-Intrinsics
 * Require manual padding + termination. Good performance though
 *
 * IsaCrypto's SHA is slower than this SHA2 SIMD, about 35%
 * https://github.com/intel/isa-l_crypto
 *
 * PicoSHA 256
 * https://github.com/okdshin/PicoSHA2
 * Non-SIMD + extra memcpy() + malloc() -> Support slow
 *
 * Random SHA retrieved from the Internet
 * https://www.officedaytime.com/simd512e/simdimg/sha256.html
 * ~4% slower, but it includes padding, .... may be this extra step adds to the overhead
 *
 * OpenSSL SHA-256
 * https://github.com/openssl/openssl/tree/openssl-3.1.2
 * Same speed, includes all padding, terminating bit blah blah -> Should use this
 *
 * Git SHA-256
 * https://github.com/git/git/blob/master/sha256/block/sha256.h
 * No SIMD -> slow
 */

static void BM_SHA2SIMD(benchmark::State &state) {
  leanstore::storage::blob::BlobState blob;
  uint8_t data[state.range(0)]     = {0};
  data[0]                          = 0x80;
  std::span<const uint8_t> payload = {data, static_cast<size_t>(state.range(0))};

  for (auto _ : state) { blob.CalculateSHA256(payload); }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(state.range(0)));
}

static void BM_PicoSHA(benchmark::State &state) {
  std::array<uint8_t, SHA256_DIGEST_SIZE> sha2_val;
  uint8_t data[state.range(0)]     = {0};
  std::span<const uint8_t> payload = {data, static_cast<size_t>(state.range(0))};

  for (auto _ : state) { picosha2::hash256(payload.begin(), payload.end(), sha2_val); }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(state.range(0)));
}

static void BM_RandomSHA(benchmark::State &state) {
  std::array<uint8_t, SHA256_DIGEST_SIZE> sha2_val;
  uint8_t data[state.range(0)] = {0};
  SHA256H msg;

  for (auto _ : state) {
    msg.Update(data, static_cast<size_t>(state.range(0)));
    msg.Final(sha2_val.data());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(state.range(0)));
}

static void BM_OpenSSL(benchmark::State &state) {
  std::array<uint8_t, SHA256_DIGEST_SIZE> sha2_val;
  uint8_t data[state.range(0)] = {0};
  auto ctx                     = EVP_MD_CTX_create();

  for (auto _ : state) {
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, static_cast<size_t>(state.range(0)));
    EVP_DigestFinal_ex(ctx, sha2_val.data(), NULL);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(state.range(0)));
}

static void BM_GitSHA(benchmark::State &state) {
  std::array<uint8_t, SHA256_DIGEST_SIZE> sha2_val;
  uint8_t data[state.range(0)] = {0};
  blk_SHA256_CTX sha256;

  for (auto _ : state) {
    blk_SHA256_Init(&sha256);
    blk_SHA256_Update(&sha256, data, static_cast<size_t>(state.range(0)));
    blk_SHA256_Final(sha2_val.data(), &sha256);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(state.range(0)));
}

static void BM_GcryptSHA(benchmark::State &state) {
  std::array<uint8_t, SHA256_DIGEST_SIZE> sha2_val;
  uint8_t data[state.range(0)] = {0};
  gcry_md_hd_t sha256;

  for (auto _ : state) {
    [[maybe_unused]] auto rc = gcry_md_open(&sha256, GCRY_MD_SHA256, 0);
    assert(rc == 0);
    gcry_md_write(sha256, data, static_cast<size_t>(state.range(0)));
    memcpy(sha2_val.data(), gcry_md_read(sha256, GCRY_MD_SHA256), SHA256_DIGEST_SIZE);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(state.range(0)));
}

BENCHMARK(BM_SHA2SIMD)->Range(64, 64 << 10);
BENCHMARK(BM_PicoSHA)->Range(64, 64 << 10);
BENCHMARK(BM_RandomSHA)->Range(64, 64 << 10);
BENCHMARK(BM_OpenSSL)->Range(64, 64 << 10);
BENCHMARK(BM_GitSHA)->Range(64, 64 << 10);
BENCHMARK(BM_GcryptSHA)->Range(64, 64 << 10);

BENCHMARK_MAIN();