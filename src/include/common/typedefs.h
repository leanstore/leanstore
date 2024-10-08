#pragma once

#include <cstdint>

using cstr = const char *;
// -------------------------------------------------------------------------------------
using u8   = uint8_t;
using u16  = uint16_t;
using u32  = uint32_t;
using u64  = uint64_t;
using u128 = unsigned __int128;

template <u8 BIT_LENGTH>
struct CustomUint {
  u64 v : BIT_LENGTH;

  friend auto operator==(const CustomUint &v1, const u64 &v2) noexcept -> bool { return v1.v == v2; }

  auto operator=(const u64 &other) -> CustomUint & {
    v = other;
    return *this;
  }
} __attribute__((packed));

using u40 = CustomUint<40>;
using u56 = CustomUint<56>;
static_assert(sizeof(u40) == 5);
static_assert(sizeof(u56) == 7);
// -------------------------------------------------------------------------------------
using i8  = int8_t;
using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;
// -------------------------------------------------------------------------------------
using pageid_t    = u64;          // Page ID
using extidx_t    = u8;           // Extent Index
using timestamp_t = u64;          // Timestamp
using txnid_t     = timestamp_t;  // txn id is actually the start ts of that txn
using wid_t       = u16;          // Worker ID
// -------------------------------------------------------------------------------------
using leng_t    = u16;
using uoffset_t = u16;
// -------------------------------------------------------------------------------------
