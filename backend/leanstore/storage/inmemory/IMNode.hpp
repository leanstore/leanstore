#pragma once
#include "Units.hpp"
#include "leanstore/sync-primitives/Latch.hpp"

namespace leanstore {
namespace storage {
namespace inmemory {

struct IMNode {
   struct Entry {
      u8* key;
      u16 key_length;
      u8* value;
      u16 value_length;
      Entry* next;
   };

   HybridLatch latch;
   Entry* entries = nullptr;
   u64 count = 0;

   IMNode();
   ~IMNode();
   
   Entry* find(const u8* key, u16 key_length);
   bool insert(u8* key, u16 key_length, u8* value, u16 value_length);
   bool remove(const u8* key, u16 key_length);
};

}}} // namespace 