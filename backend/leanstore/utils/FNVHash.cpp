#include "FNVHash.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
// -------------------------------------------------------------------------------------
{
u64 FNV::hash(u64 val)
{
   // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
   u64 hash_val = FNV_OFFSET_BASIS_64;
   for (int i = 0; i < 8; i++) {
      u64 octet = val & 0x00ff;
      val = val >> 8;

      hash_val = hash_val ^ octet;
      hash_val = hash_val * FNV_PRIME_64;
   }
   return hash_val;
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
