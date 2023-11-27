#pragma once

#include <cstdint>

using cstr = const char *;
// -------------------------------------------------------------------------------------
using u8   = uint8_t;
using u16  = uint16_t;
using u32  = uint32_t;
using u64  = uint64_t;
using u128 = unsigned __int128;
// -------------------------------------------------------------------------------------
using i8  = int8_t;
using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;
// -------------------------------------------------------------------------------------
using pageid_t    = u64;          // Page ID
using extidx_t    = u8;           // Extent Index
using logid_t     = u64;          // Log ID
using timestamp_t = u64;          // Transaction Time Stamp
using txnid_t     = timestamp_t;  // txn id is actually the start ts of that txn
using wid_t       = u32;          // Worker ID
// The index ID inside leanstore, i.e. the slot_id of the index root in MetadataPage
using indexid_t = u16;
// -------------------------------------------------------------------------------------
using leng_t = u16;
// -------------------------------------------------------------------------------------
