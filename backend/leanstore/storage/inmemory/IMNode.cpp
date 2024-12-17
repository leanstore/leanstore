#include "IMNode.hpp"
#include <cstring>

namespace leanstore {
namespace storage {
namespace inmemory {

IMNode::IMNode() {}

IMNode::~IMNode() {
   Entry* current = entries;
   while (current) {
      Entry* next = current->next;
      delete[] current->key;
      delete[] current->value;
      delete current;
      current = next;
   }
}

IMNode::Entry* IMNode::find(const u8* key, u16 key_length) {
   Entry* current = entries;
   while (current) {
      if (key_length == current->key_length && 
          memcmp(key, current->key, key_length) == 0) {
         return current;
      }
      current = current->next;
   }
   return nullptr;
}

bool IMNode::insert(u8* key, u16 key_length, u8* value, u16 value_length) {
   if (find(key, key_length)) {
      return false;
   }
   
   Entry* new_entry = new Entry();
   new_entry->key = new u8[key_length];
   new_entry->value = new u8[value_length];
   memcpy(new_entry->key, key, key_length);
   memcpy(new_entry->value, value, value_length);
   new_entry->key_length = key_length;
   new_entry->value_length = value_length;
   
   new_entry->next = entries;
   entries = new_entry;
   count++;
   return true;
}

bool IMNode::remove(const u8* key, u16 key_length) {
   Entry** current = &entries;
   while (*current) {
      if (key_length == (*current)->key_length &&
          memcmp(key, (*current)->key, key_length) == 0) {
         Entry* to_delete = *current;
         *current = to_delete->next;
         delete[] to_delete->key;
         delete[] to_delete->value;
         delete to_delete;
         count--;
         return true;
      }
      current = &(*current)->next;
   }
   return false;
}

}}} // namespace 