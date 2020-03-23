#pragma once
// -------------------------------------------------------------------------------------
#include <stddef.h>
#include <stdint.h>

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
// -------------------------------------------------------------------------------------
using std::atomic;
using std::cerr;
using std::cout;
using std::endl;
using std::make_unique;
using std::string;
using std::to_string;
using std::tuple;
using std::unique_ptr;
// -------------------------------------------------------------------------------------
using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using u128 = unsigned __int128;
// -------------------------------------------------------------------------------------
using s8 = int8_t;
using s16 = int16_t;
using s32 = int32_t;
using s64 = int64_t;
// -------------------------------------------------------------------------------------
using SIZE = size_t;
using PID = u64;
using DTID = u64;  // Datastructure ID
// -------------------------------------------------------------------------------------
using TINYINT = s8;
using SMALLINT = s16;
using INTEGER = s32;
using UINTEGER = u32;
using DOUBLE = double;
using STRING = string;
using BITMAP = u8;
// -------------------------------------------------------------------------------------
using str = std::string_view;
// -------------------------------------------------------------------------------------
using BytesArray = std::unique_ptr<u8[]>;
// -------------------------------------------------------------------------------------
template <int s>
struct getTheSizeOf;
// -------------------------------------------------------------------------------------
