#include "typefold/typefold.h"

// -------------------------------------------------------------------------------------
// Fold functions convert all types to a lexicographical comparable format
auto Fold(uint8_t *writer, const Integer &x) -> uint16_t {
  *reinterpret_cast<uint32_t *>(writer) = __builtin_bswap32(x ^ (1UL << 31));
  return sizeof(x);
}

// -------------------------------------------------------------------------------------
auto Fold(uint8_t *writer, const UInteger &x) -> uint16_t {
  *reinterpret_cast<uint32_t *>(writer) = __builtin_bswap32(x);
  return sizeof(x);
}

// -------------------------------------------------------------------------------------
auto Fold(uint8_t *writer, const Timestamp &x) -> uint16_t {
  *reinterpret_cast<UniqueID *>(writer) = __builtin_bswap64(x ^ (1ULL << 63));
  return sizeof(x);
}

// -------------------------------------------------------------------------------------
auto Fold(uint8_t *writer, const UniqueID &x) -> uint16_t {
  *reinterpret_cast<UniqueID *>(writer) = __builtin_bswap64(x);
  return sizeof(x);
}

// -------------------------------------------------------------------------------------
template <int Leng>
auto Fold(uint8_t *writer, const Varchar<Leng> &x) -> uint16_t {
  memcpy(writer, x.data, x.length);
  writer[x.length] = 0;
  return x.length + 1;
}

// -------------------------------------------------------------------------------------
auto Unfold(const uint8_t *input, Integer &x) -> uint16_t {
  x = __builtin_bswap32(*reinterpret_cast<const uint32_t *>(input)) ^ (1UL << 31);
  return sizeof(x);
}

// -------------------------------------------------------------------------------------
auto Unfold(const uint8_t *input, uint32_t &x) -> uint16_t {
  x = __builtin_bswap32(*reinterpret_cast<const uint32_t *>(input));
  return sizeof(x);
}

// -------------------------------------------------------------------------------------
auto Unfold(const uint8_t *input, Timestamp &x) -> uint16_t {
  x = __builtin_bswap64(*reinterpret_cast<const UniqueID *>(input)) ^ (1UL << 63);
  return sizeof(x);
}

// -------------------------------------------------------------------------------------
auto Unfold(const uint8_t *input, UniqueID &x) -> uint16_t {
  x = __builtin_bswap64(*reinterpret_cast<const UniqueID *>(input));
  return sizeof(x);
}

// -------------------------------------------------------------------------------------
template <int Leng>
auto Unfold(const uint8_t *input, Varchar<Leng> &x) -> uint16_t {
  int l = strlen(reinterpret_cast<const char *>(input));
  assert(l <= Leng);
  std::memcpy(x.data, input, l);
  x.length = l;
  return l + 1;
}

template auto Fold<15>(uint8_t *writer, const Varchar<15> &x) -> uint16_t;
template auto Unfold<15>(const uint8_t *input, Varchar<15> &x) -> uint16_t;
template auto Fold<16>(uint8_t *writer, const Varchar<16> &x) -> uint16_t;
template auto Unfold<16>(const uint8_t *input, Varchar<16> &x) -> uint16_t;
template auto Fold<128>(uint8_t *writer, const Varchar<128> &x) -> uint16_t;
template auto Unfold<128>(const uint8_t *input, Varchar<128> &x) -> uint16_t;