#pragma once
// -------------------------------------------------------------------------------------
#include <stdint.h>
#include <stddef.h>
#include <iostream>
#include <string>
#include <memory>
#include <cassert>
// -------------------------------------------------------------------------------------
#define NULL_CODE_MARGIN 1
// -------------------------------------------------------------------------------------
using std::cerr;
using std::cout;
using std::string;
using std::endl;
using std::unique_ptr;
using std::make_unique;
using std::tuple;
using std::atomic;
using std::to_string;
// -------------------------------------------------------------------------------------
using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
// -------------------------------------------------------------------------------------
using s8 = int8_t;
using s16 = int16_t;
using s32 = int32_t;
using s64 = int64_t;
// ------------------------------------------------------------------------------------- 
using SIZE = size_t;
using PID = u64;
using DTID = u64; // Datastructure ID
// -------------------------------------------------------------------------------------
enum class ColumnType {
   // TODO
};
using TINYINT = s8;
using SMALLINT = s16;
using INTEGER = s32; // we use FOR always at the beginning so negative integers will be handled out
using UINTEGER = u32;
using DOUBLE = double;
using STRING = string;
using BITMAP = u8;
// -------------------------------------------------------------------------------------
using str = std::string_view;
// -------------------------------------------------------------------------------------
using BytesArray = std::unique_ptr<u8[]>;
// -------------------------------------------------------------------------------------
template<int s>
struct getTheSizeOf;
// -------------------------------------------------------------------------------------
