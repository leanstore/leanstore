#pragma once
// -------------------------------------------------------------------------------------
#include <Exceptions.hpp>
#include <stdint.h>
#include <stddef.h>
#include <iostream>
#include <string>
#include <memory>
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
   INTEGER,
   DOUBLE,
   STRING,
   SKIP, // SKIP THIS COLUMN
   // The next types are out of scope
   FLOAT,
   BIGINT,
   SMALLINT,
   UNDEFINED
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
inline ColumnType ConvertStringToType(const string type_str)
{
   if ( type_str == "integer" )
      return ColumnType::INTEGER;
   else if ( type_str == "double" )
      return ColumnType::DOUBLE;
   else if ( type_str == "string" )
      return ColumnType::STRING;
   else if ( type_str == "skip" )
      return ColumnType::SKIP;
   else
      return ColumnType::SKIP;
};
// ---------1----------------------------------------------------------------------------
inline string ConvertTypeToString(const ColumnType type_str)
{
   if ( type_str == ColumnType::INTEGER )
      return "integer";
   else if ( type_str == ColumnType::DOUBLE )
      return "double";
   else if ( type_str == ColumnType::STRING )
      return "string";
   else
      UNREACHABLE();
   return "";
};
// -------------------------------------------------------------------------------------
using BytesArray = std::unique_ptr<u8[]>;
// -------------------------------------------------------------------------------------
template<int s>
struct getTheSizeOf;
// -------------------------------------------------------------------------------------
#define check(expr) if (!(expr)) { perror(#expr); assert(false); }
#ifdef NDEBUG
#define rassert(expr) if(!(expr)) { throw std::runtime_error(std::string(__FILE__)+ "@"+std::to_string( __LINE__));}
#endif
#ifndef NDEBUG
#define rassert(expr) assert(expr)
#endif