/**
 * @file schema.hpp
 * @brief defines Schema of each relation.
 *
 */
#include "../shared/Types.hpp"

struct scaled_t {
   // Entries: 1 to 1 160 000 * scale
   static constexpr int id = 0;
   struct Key {
      static constexpr int id = 0;
      Integer scaled_key;
   };
   Integer s_u_key;      // references unscaled: scaled_key % |unscaled| + 1
   Varchar<11> s_color;  // random color
   Numeric s_num;        // random Numeric
   // -------------------------------------------------------------------------------------
   template <class T>
   static unsigned foldKey(uint8_t* out, const T& key)
   {
      unsigned pos = 0;
      pos += fold(out + pos, key.scaled_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& key)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, key.scaled_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::scaled_key); };
};

struct unscaled_t {
   // Entries 1 to 100
   static constexpr int id = 1;
   struct Key {
      static constexpr int id = 1;
      Integer unscaled_key;
   };
   Integer u_int;        // random Integer
   Varchar<11> u_color;  // random color
   Numeric u_num;        // random Numeric

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& key)
   {
      unsigned pos = 0;
      pos += fold(out + pos, key.unscaled_key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& key)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, key.unscaled_key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::unscaled_key); };
};
