#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <emmintrin.h>
#include <fcntl.h>
#include <linux/futex.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_bool(seq, false, "");
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
struct Tuple {
   u64 a1;
   u32 a2;
   char a3[10];
   s32 a4;
};
// -------------------------------------------------------------------------------------
// #define WAL(type, m1) [](const u8* tuple, u8* entry) { }
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
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

#define entrySize1(Type, A1) ((2 * sizeof(u16)) + sizeof(Type::A1))
#define entrySize2(Type, A1, A2) entrySize1(Type, A1) + entrySize1(Type, A2)
#define entrySize3(Type, A1, A2, A3) entrySize1(Type, A1) + entrySize1(Type, A2) + entrySize1(Type, A3)
#define entrySize4(Type, A1, A2, A3, A4) entrySize1(Type, A1) + entrySize1(Type, A2) + entrySize1(Type, A3) + entrySize1(Type, A4)

   struct WALInfo {
      std::function<void(u8*, u8*)> before;
      std::function<void(u8*, u8*)> after;
      u16 entry_size;
   };

#define WAL1(Type, A1)                                                                                        \
   {                                                                                                          \
      .before = beforeWrapper1(Type, A1), .after = afterWrapper1(Type, A1), .entry_size = entrySize(Type, A1) \
   }
#define WAL2(Type, A1, A2) beforeWrapper2(Type, A1, A2), afterWrapper2(Type, A1, A2), entrySize2(Type, A1, A2)
#define WAL3(Type, A1, A2, A3) beforeWrapper3(Type, A1, A2, A3), afterWrapper3(Type, A1, A2, A3), entrySize3(Type, A1, A2, A3)
#define WAL4(Type, A1, A2, A3, A4) beforeWrapper4(Type, A1, A2, A3, A4), afterWrapper4(Type, A1, A2, A3, A4), entrySize3(Type, A1, A2, A3, A4)

   auto before = beforeWrapper4(Tuple, a1, a2, a3, a4);
   auto after = afterWrapper4(Tuple, a1, a2, a3, a4);

   auto reverse = [](u8* tuple, u8* entry, u8* entry_end) {
      while (entry < entry_end) {
         u16 offset = *reinterpret_cast<u16*>(entry);
         entry += 2;
         u16 size = *reinterpret_cast<u16*>(entry);
         entry += 2;
         for (u16 b_i = 0; b_i < size; b_i++) {
            *(tuple + offset + b_i) ^= *(entry + b_i);
         }
         entry += size;
      }
   };
   u8 entry[128];
   Tuple t;
   t.a1 = 10;
   t.a2 = 16;
   const char* adnan = "Adnan";
   const char* viktor = "Victor";
   std::memcpy(t.a3, adnan, 6);
   t.a4 = -128;
   // -------------------------------------------------------------------------------------
   before(reinterpret_cast<u8*>(&t), entry);
   cout << *reinterpret_cast<u16*>(entry) << endl;
   cout << *reinterpret_cast<u16*>(entry + 2) << endl;
   cout << *reinterpret_cast<u64*>(entry + 4) << endl;
   cout << *reinterpret_cast<u16*>(entry + 4 + 8) << endl;
   cout << *reinterpret_cast<u16*>(entry + 4 + 8 + 2) << endl;
   cout << *reinterpret_cast<u32*>(entry + 4 + 8 + 2 + 2) << endl;
   t.a1 = 20;
   t.a2 = t.a1 * 5;
   std::memcpy(t.a3, viktor, 7);
   t.a4 = 512;
   after(reinterpret_cast<u8*>(&t), entry);
   cout << "After" << endl;
   cout << *reinterpret_cast<u16*>(entry) << endl;
   cout << *reinterpret_cast<u16*>(entry + 2) << endl;
   cout << *reinterpret_cast<u64*>(entry + 4) << endl;
   cout << *reinterpret_cast<u16*>(entry + 4 + 8) << endl;
   cout << *reinterpret_cast<u16*>(entry + 4 + 8 + 2) << endl;
   cout << *reinterpret_cast<u32*>(entry + 4 + 8 + 2 + 2) << endl;
   // -------------------------------------------------------------------------------------
   reverse(reinterpret_cast<u8*>(&t), entry, entry + entrySize4(Tuple, a1, a2, a3, a4));
   cout << t.a1 << endl;
   cout << t.a2 << endl;
   cout << t.a3 << endl;
   cout << t.a4 << endl;
   reverse(reinterpret_cast<u8*>(&t), entry, entry + entrySize4(Tuple, a1, a2, a3, a4));
   cout << t.a1 << endl;
   cout << t.a2 << endl;
   cout << t.a3 << endl;
   cout << t.a4 << endl;
   reverse(reinterpret_cast<u8*>(&t), entry, entry + entrySize4(Tuple, a1, a2, a3, a4));
   cout << t.a1 << endl;
   cout << t.a2 << endl;
   cout << t.a3 << endl;
   cout << t.a4 << endl;
   /* Target
   storage.update(
                key,
                t,
                [&](Tuple &t) {
                  t.a1 = 20;
                  t.a2 = t.a1 * 2;
                },
                MACRO(Tuple::a1, Tuple::a2)
     );
   */
   // -------------------------------------------------------------------------------------
   return 0;
}
