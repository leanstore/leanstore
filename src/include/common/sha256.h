#pragma once

#include <memory.h>
#include <x86intrin.h>
#include <cstdint>
#include <cstring>

namespace leanstore {

/**
 * @brief Steal and adapt from
 *
 * https://www.officedaytime.com/simd512e/simdimg/sha256.html
 */
class SHA256H {
 public:
  static constexpr size_t MBYTES      = 64;
  static constexpr size_t DIGEST_SIZE = 32;

  SHA256H();
  ~SHA256H() = default;

  void Initialize();
  void Update(const void *buf, size_t length);
  void Serialize(uint64_t *s);
  void Final(void *digest);

 protected:
  // Initial hash value (see FIPS 180-4 5.3.3)
  static constexpr uint64_t H0 = 0x6a09e667;
  static constexpr uint64_t H1 = 0xbb67ae85;
  static constexpr uint64_t H2 = 0x3c6ef372;
  static constexpr uint64_t H3 = 0xa54ff53a;
  static constexpr uint64_t H4 = 0x510e527f;
  static constexpr uint64_t H5 = 0x9b05688c;
  static constexpr uint64_t H6 = 0x1f83d9ab;
  static constexpr uint64_t H7 = 0x5be0cd19;

  unsigned char msgbuf_[MBYTES];
  size_t msgbuf_count_;   // length (in byte) of the data currently in the message block
  uint64_t total_count_;  // total length (in byte) of the message

  /**
   * @brief Intermediate hash
   * This can be extracted using the following
   *
   * uint16_t val[8];
   * memcpy(val, &h0145, sizeof(val));
   */
  __m128i h0145_;  // h0:h1:h4:h5
  __m128i h2367_;  // h2:h3:h6:h7

  void ProcessMsgBlock(const unsigned char *msg);
};

// K Array (see FIPS 180-4 4.2.2)
static constexpr union {
  uint32_t dw[64];
  __m128i x[16];
} K = {{0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2}};

}  // namespace leanstore
