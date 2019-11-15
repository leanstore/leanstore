#include "ZipfCache.hpp"
#include "Files.hpp"
#include "Units.hpp"
#include "Exceptions.hpp"
// ------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------
#include <fstream>
// ------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace utils {
// -------------------------------------------------------------------------------------
template<class ValueType>
ZipfCache<ValueType>::ZipfCache(const string &path, uint64_t max, double factor, uint64_t seed, uint64_t count)
        : file_path(path + ConfigToFileName(max, factor, seed, count))
          , count(count)
{
}
// -------------------------------------------------------------------------------------
template<class ValueType>
void ZipfCache<ValueType>::Add(const vector<ValueType> &values)
{
   ensure(!Has());

   ofstream out(file_path);
   out.write((char *) &values[0], count * sizeof(ValueType));
}
// -------------------------------------------------------------------------------------
template<class ValueType>
bool ZipfCache<ValueType>::Has() const
{
   return fileExists(file_path);
}
// -------------------------------------------------------------------------------------
template<class ValueType>
vector<ValueType> ZipfCache<ValueType>::Get() const
{
   ensure(Has());

   vector<ValueType> result(count);
   ifstream in(file_path);
   in.read((char *) &result[0], count * sizeof(ValueType));
   return move(result);
}
// -------------------------------------------------------------------------------------
template<>
const string ZipfCache<uint32_t>::ConfigToFileName(uint64_t N, double factor, uint64_t seed, uint64_t count) const
{
   return "zipf_cache_ub4_" + to_string(N) + "_" + to_string(factor) + "_" + to_string(seed) + "_" + to_string(count);
}
// -------------------------------------------------------------------------------------
template<>
const string ZipfCache<uint64_t>::ConfigToFileName(uint64_t N, double factor, uint64_t seed, uint64_t count) const
{
   return "zipf_cache_ub8_" + to_string(N) + "_" + to_string(factor) + "_" + to_string(seed) + "_" + to_string(count);
}
// -------------------------------------------------------------------------------------
template ZipfCache<uint32_t>::ZipfCache(const string &, uint64_t, double, uint64_t, uint64_t);
template ZipfCache<uint64_t>::ZipfCache(const string &, uint64_t, double, uint64_t, uint64_t);

template void ZipfCache<uint32_t>::Add(const vector<uint32_t> &values);
template void ZipfCache<uint64_t>::Add(const vector<uint64_t> &values);

template bool ZipfCache<uint32_t>::Has() const;
template bool ZipfCache<uint64_t>::Has() const;

template vector<uint32_t> ZipfCache<uint32_t>::Get() const;
template vector<uint64_t> ZipfCache<uint64_t>::Get() const;
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
