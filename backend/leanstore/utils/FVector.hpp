#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
template <class T>
struct FVector {
   uint64_t count;
   int fd;
   T* data;

   FVector() : data(nullptr) {}
   FVector(const char* pathname) { readBinary(pathname); }
   ~FVector()
   {
      if (data) {
         posix_check(munmap(reinterpret_cast<void*>(data), count * sizeof(T)) == 0);
         posix_check(close(fd) == 0);
      }
   }

   void readBinary(const char* pathname)
   {
      fd = open(pathname, O_RDONLY);
      if (fd == -1) {
         cout << pathname << endl;
      }
      posix_check(fd != -1);
      struct stat sb;
      posix_check(fstat(fd, &sb) != -1);
      count = static_cast<uint64_t>(sb.st_size) / sizeof(T);
      data = reinterpret_cast<T*>(mmap(nullptr, count * sizeof(T), PROT_READ, MAP_PRIVATE, fd, 0));
      posix_check(data != MAP_FAILED);
   }

   uint64_t size() const { return count; }
   T operator[](std::size_t idx) const { return data[idx]; }
};
// -------------------------------------------------------------------------------------
template <class T>
void writeBinary(const char* pathname, std::vector<T>& v)
{
   int fd = open(pathname, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
   posix_check(fd != -1);
   uint64_t length = v.size() * sizeof(T);
   posix_check(posix_fallocate(fd, 0, length) == 0);
   T* data = reinterpret_cast<T*>(mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
   posix_check(data != MAP_FAILED);
   memcpy(data, v.data(), length);
   posix_check(close(fd) == 0);
}
// -------------------------------------------------------------------------------------
typedef struct {
   uint64_t size;
   uint64_t offset;
} StringIndexSlot;
template <>
struct FVector<std::string_view> {
   struct Data {
      uint64_t count;
      StringIndexSlot slot[];
   };

   uint64_t fileSize;
   int fd;
   Data* data;

   FVector() : data(nullptr) {}
   FVector(const char* pathname) { readBinary(pathname); }
   ~FVector()
   {
      if (data) {
         posix_check(munmap(data, fileSize) == 0);
         posix_check(close(fd) == 0);
      }
   }

   void readBinary(const char* pathname)
   {
      fd = open(pathname, O_RDONLY);
      if (fd == -1) {
         cout << pathname << endl;
      }
      posix_check(fd != -1);
      struct stat sb;
      posix_check(fstat(fd, &sb) != -1);
      fileSize = static_cast<uint64_t>(sb.st_size);
      data = reinterpret_cast<Data*>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
      posix_check(data != MAP_FAILED);
   }

   uint64_t size() { return data->count; }
   std::string_view operator[](std::size_t idx) const
   {
      auto slot = data->slot[idx];
      return std::string_view(reinterpret_cast<char*>(data) + slot.offset, slot.size);
   }
};
// -------------------------------------------------------------------------------------
template <typename T>
void fillVectorFromBinaryFile(const char* pathname, std::vector<T>& v)
{
   int fd = open(pathname, O_RDONLY);
   if (fd == -1) {
      cout << pathname << endl;
   }
   posix_check(fd != -1);
   struct stat sb;
   posix_check(fstat(fd, &sb) != -1);
   auto fileSize = static_cast<u64>(sb.st_size);
   auto data = reinterpret_cast<T*>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
   posix_check(data != MAP_FAILED);
   v.resize(fileSize / sizeof(T));
   memcpy(v.data(), data, fileSize);
}
// -------------------------------------------------------------------------------------
void writeBinary(const char* pathname, std::vector<std::string>& v);
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
