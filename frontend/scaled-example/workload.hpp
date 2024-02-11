/**
 * @file workload.hpp
 * @brief Functions to execute minimal example.
 *
 */

#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>

DEFINE_bool(order_wdc_index, true, "");
// 0: Partid, 1: quantity, 2: orderdate, 3: tax, 4: discount; 5: cust_key, 6: priority, 7: ship priority,
typedef std::tuple<Integer, Integer, Integer, Integer, Integer, Integer, Varchar<15>, Varchar<1>> commonOrderParts;
atomic<u64> scanned_elements(0);

// load

u32 scale;
Integer StartDate = 1;
Integer EndDate;
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
// RandomGenerator functions
// -------------------------------------------------------------------------------------

// [0, n)
Integer rnd(Integer n)
{
   return leanstore::utils::RandomGenerator::getRand(0, n);
}
// [low, high]
Integer uRand(Integer low, Integer high)
{
   return rnd(high - low + 1) + low;
}

template <typename type>
type pickOne(vector<type> values, Integer i)
{
   int random = i % values.size();
   return values[random];
}

// -------------------------------------------------------------------------------------
// Functions to scale Datasets
// -------------------------------------------------------------------------------------
u32 getScale(Integer scale)
{
   return scale * 1'160'000;
}
Integer unscaledScale = 100;
// -------------------------------------------------------------------------------------
// Generator functions
// -------------------------------------------------------------------------------------

Integer getInt(Integer i)
{
   return std::hash<Integer>{}(i * 10);
}
Varchar<11> getColor(Integer i)
{
   vector<Varchar<10>> values = {
       "almond",    "antique",   "aquamarine", "azure",   "beige",     "bisque", "black",      "blanched", "blue",      "blush",  "brown",
       "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",  "cornflower", "cornsilk", "cream",     "cyan",   "dark",
       "deep",      "dim",       "dodger",     "drab",    "firebrick", "floral", "forest",     "frosted",  "gainsboro", "ghost",  "goldenrod",
       "green",     "grey",      "honeydew",   "hot",     "indian",    "ivory",  "khaki",      "lace",     "lavender",  "lawn",   "lemon",
       "light",     "lime",      "linen",      "magenta", "maroon",    "medium", "metallic",   "midnight", "mint",      "misty",  "moccasin",
       "navajo",    "navy",      "olive",      "orange",  "orchid",    "pale",   "papaya",     "peach",    "peru",      "pink",   "plum",
       "powder",    "puff",      "purple",     "red",     "rose",      "rosy",   "royal",      "saddle",   "salmon",    "sandy",  "seashell",
       "sienna",    "sky",       "slate",      "smoke",   "snow",      "spring", "steel",      "tan",      "thistle",   "tomato", "turquoise",
       "violet",    "wheat",     "white",      "yellow"};
   return pickOne<Varchar<10>>(values, i);
}
Numeric getNumeric(Integer i)
{
   return getInt(i) / 100.0;
}
Integer getUnscaled(Integer i)
{
   return (i % unscaledScale) + 1;
}

// -------------------------------------------------------------------------------------
// Functions to add data to dataset e.g. initialize dataset
// -------------------------------------------------------------------------------------
bool checkUnscaled();
void loadUnscaled()
{
   for (Integer i = 1; i <= unscaledScale; i++) {
      unscaled_table.insert({i}, {getInt(i), getColor(i), getNumeric(i)});
   }
}
bool checkScaled(u32 scaleFactor);
void loadScaled(u32 scaleFactor)
{
   Integer start = getScale(scaleFactor - 1);
   Integer end = getScale(scaleFactor);
   for (Integer i = start; i < end; i++) {
      scaled_table.insert({i}, {getUnscaled(i), getColor(i), getNumeric(i)});
   }
}
// -------------------------------------------------------------------------------------
// Functions to execute operations on data
// -------------------------------------------------------------------------------------

bool checkOneUnscaled(Integer i)
{
   assert(i >= 1);
   assert(i <= unscaledScale);
   bool success = false;
   unscaled_table.lookup1({i}, [&](const unscaled_t& record) {
      success = true;
      if (record.u_color != getColor(i))
         success = false;
      if (record.u_int != getInt(i))
         success = false;
      if (record.u_num != getNumeric(i))
         success = false;
   });
   if (!success) {
      cout << "Unscaled Failed: " << i << endl;
   }
   return success;
}

bool checkUnscaled()
{
   for (Integer i = 1; i <= unscaledScale; i++) {
      if (!checkOneUnscaled(i)) {
         return false;
      }
   }
   return true;
}
void scanUnscaled()
{
   unscaled_table.scan(
       {},
       [&](const unscaled_t::Key&, const unscaled_t&) {
          // cout << key.unscaled_key << " " << record.u_color.toString().c_str() << endl;
          return true;
       },
       [&]() {});
}

bool checkOneScaled(Integer i)
{
   Integer start = getScale(0);
   Integer end = getScale(scale);
   assert(i >= start);
   assert(i < end);
   bool success = true;
   scaled_table.lookup1({i}, [&](const scaled_t& record) {
      if (record.s_color != getColor(i))
         success = false;
      if (record.s_num != getNumeric(i))
         success = false;
      if (record.s_u_key != getUnscaled(i))
         success = false;
      if (!checkOneUnscaled(record.s_u_key))
         success = false;
   });
   if (!success) {
      cout << "Scaled Failed: " << i << endl;
   }
   return success;
}
bool checkScaled(u32 scaleFactor)
{
   Integer start = getScale(scaleFactor - 1);
   Integer end = getScale(scaleFactor);
   for (Integer i = start; i < end; i++) {
      if (!checkOneScaled(i)) {
         cout << "Scaled Failed: " << i << endl;
         return false;
      }
   }
   return true;
}
void fetchOneScaled()
{
   Integer id = uRand(getScale(0), getScale(scale) - 1);
   checkOneScaled(id);
}
void fetchOneUnscaled()
{
   Integer id = uRand(1, unscaledScale);
   checkOneUnscaled(id);
}

void runOneQuery()
{
   fetchOneScaled();
}

void checkScale(u32 scale)
{
   for (u32 i = getScale(scale - 1); i < getScale(scale); i++) {
      assert(checkOneScaled(i));
   }
}