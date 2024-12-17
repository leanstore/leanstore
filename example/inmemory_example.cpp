#include "leanstore/LeanStore.hpp"

int main() {
   LeanStore db;
   auto& store = db.registerIMStore("my_store");
   
   // Insert
   u8 key[] = "hello";
   u8 value[] = "world";
   store.insert(key, 5, value, 5);
   
   // Lookup
   store.lookup(key, 5, [](const u8* payload, u16 payload_length) {
      std::cout << std::string((char*)payload, payload_length) << std::endl;
   });
   
   return 0;
} 