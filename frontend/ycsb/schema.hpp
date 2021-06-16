/**
 * @file schema.hpp
 * @brief defines Schema of each relation.
 *
 */
#include "../shared/Types.hpp"

template<typename TableKey, typename TablePayload>
struct relation {
   // Entries: 1 to 1 160 000 * scale
   static constexpr int id = 0;
   struct Key {
      static constexpr int id = 0;
      TableKey my_key;
   };
   TablePayload my_payload;  //
   // -------------------------------------------------------------------------------------
   template <class T>
   static unsigned foldKey(uint8_t* out, const T& key)
   {
      unsigned pos = 0;
      pos += fold(out + pos, key.my_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& key)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, key.my_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::my_key); };
};
