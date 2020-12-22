#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <cstring>
// -------------------------------------------------------------------------------------
// TODO: Works only for update same size
#define DELTA_COPY
#ifdef DELTA_XOR
// Obsolete
#define beforeBody(Type, Attribute, tuple, entry)                    \
   const auto Attribute##_offset = offsetof(Type, Attribute);        \
   const auto Attribute##_size = sizeof(Type::Attribute);            \
   *reinterpret_cast<u16*>(entry) = Attribute##_offset;              \
   entry += sizeof(u16);                                             \
   *reinterpret_cast<u16*>(entry) = Attribute##_size;                \
   entry += sizeof(u16);                                             \
   std::memcpy(entry, tuple + Attribute##_offset, Attribute##_size); \
   entry += Attribute##_size;

#define afterBody(Type, Attribute, tuple, entry)              \
   const auto Attribute##_offset = offsetof(Type, Attribute); \
   const auto Attribute##_size = sizeof(Type::Attribute);     \
   entry += (sizeof(u16) * 2);                                \
   for (u64 b_i = 0; b_i < Attribute##_size; b_i++) {         \
      *(entry + b_i) ^= *(tuple + Attribute##_offset + b_i);  \
   }                                                          \
   entry += Attribute##_size;
#endif
#ifdef DELTA_COPY
#define beforeBody(Type, Attribute, tuple, entry)                    \
   const auto Attribute##_offset = offsetof(Type, Attribute);        \
   const auto Attribute##_size = sizeof(Type::Attribute);            \
   *reinterpret_cast<u16*>(entry) = Attribute##_offset;              \
   entry += sizeof(u16);                                             \
   *reinterpret_cast<u16*>(entry) = Attribute##_size;                \
   entry += sizeof(u16);                                             \
   std::memcpy(entry, tuple + Attribute##_offset, Attribute##_size); \
   entry += 2 * Attribute##_size;

#define afterBody(Type, Attribute, tuple, entry)                     \
   const auto Attribute##_offset = offsetof(Type, Attribute);        \
   const auto Attribute##_size = sizeof(Type::Attribute);            \
   entry += (sizeof(u16) * 2);                                       \
   entry += Attribute##_size;                                        \
   std::memcpy(entry, tuple + Attribute##_offset, Attribute##_size); \
   entry += 1 * Attribute##_size;
#endif

#define beforeWrapper1(Type, A1) [](u8* tuple, u8* entry) { beforeBody(Type, A1, tuple, entry); }
#define beforeWrapper2(Type, A1, A2)      \
   [](u8* tuple, u8* entry) {             \
      beforeBody(Type, A1, tuple, entry); \
      beforeBody(Type, A2, tuple, entry); \
   }
#define beforeWrapper3(Type, A1, A2, A3)  \
   [](u8* tuple, u8* entry) {             \
      beforeBody(Type, A1, tuple, entry); \
      beforeBody(Type, A2, tuple, entry); \
      beforeBody(Type, A3, tuple, entry); \
   }
#define beforeWrapper4(Type, A1, A2, A3, A4) \
   [](u8* tuple, u8* entry) {                \
      beforeBody(Type, A1, tuple, entry);    \
      beforeBody(Type, A2, tuple, entry);    \
      beforeBody(Type, A3, tuple, entry);    \
      beforeBody(Type, A4, tuple, entry);    \
   }

#define afterWrapper1(Type, A1) [](u8* tuple, u8* entry) { afterBody(Type, A1, tuple, entry); }
#define afterWrapper2(Type, A1, A2)      \
   [](u8* tuple, u8* entry) {            \
      afterBody(Type, A1, tuple, entry); \
      afterBody(Type, A2, tuple, entry); \
   }

#define afterWrapper3(Type, A1, A2, A3)  \
   [](u8* tuple, u8* entry) {            \
      afterBody(Type, A1, tuple, entry); \
      afterBody(Type, A2, tuple, entry); \
      afterBody(Type, A3, tuple, entry); \
   }

#define afterWrapper4(Type, A1, A2, A3, A4) \
   [](u8* tuple, u8* entry) {               \
      afterBody(Type, A1, tuple, entry);    \
      afterBody(Type, A2, tuple, entry);    \
      afterBody(Type, A3, tuple, entry);    \
      afterBody(Type, A4, tuple, entry);    \
   }

#ifdef DELTA_XOR
#define entrySize1(Type, A1) ((2 * sizeof(u16)) + (1 * sizeof(Type::A1)))
#endif
#ifdef DELTA_COPY
#define entrySize1(Type, A1) ((2 * sizeof(u16)) + (2 * sizeof(Type::A1)))
#endif
#define entrySize2(Type, A1, A2) entrySize1(Type, A1) + entrySize1(Type, A2)
#define entrySize3(Type, A1, A2, A3) entrySize1(Type, A1) + entrySize1(Type, A2) + entrySize1(Type, A3)
#define entrySize4(Type, A1, A2, A3, A4) entrySize1(Type, A1) + entrySize1(Type, A2) + entrySize1(Type, A3) + entrySize1(Type, A4)

#define WALUpdate1(Type, A1)                                                  \
   {                                                                          \
      beforeWrapper1(Type, A1), afterWrapper1(Type, A1), entrySize1(Type, A1) \
   }
#define WALUpdate2(Type, A1, A2)                                                          \
   {                                                                                      \
      beforeWrapper2(Type, A1, A2), afterWrapper2(Type, A1, A2), entrySize2(Type, A1, A2) \
   }
#define WALUpdate3(Type, A1, A2, A3)                                                                  \
   {                                                                                                  \
      beforeWrapper3(Type, A1, A2, A3), afterWrapper3(Type, A1, A2, A3), entrySize3(Type, A1, A2, A3) \
   }
#define WALUpdate4(Type, A1, A2, A3, A4)                                                                          \
   {                                                                                                              \
      beforeWrapper4(Type, A1, A2, A3, A4), afterWrapper4(Type, A1, A2, A3, A4), entrySize4(Type, A1, A2, A3, A4) \
   }
