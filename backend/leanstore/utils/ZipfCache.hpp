#pragma once
// -------------------------------------------------------------------------------------
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace utils {
// -------------------------------------------------------------------------------------
template<class ValueType>
class ZipfCache {
public:
   ZipfCache(const std::string &path, uint64_t max, double factor, uint64_t seed, uint64_t count);

   void Add(const std::vector<ValueType> &values);
   bool Has() const;
   std::vector<ValueType> Get() const;

   const std::string GetFilePath() const { return file_path; }

private:
   const std::string file_path;
   const uint64_t count;

   const std::string ConfigToFileName(uint64_t max, double factor, uint64_t seed, uint64_t count) const;
};
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------