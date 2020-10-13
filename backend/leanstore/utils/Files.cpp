#include "Files.hpp"

#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cmath>
#include <sstream>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
template <class T>
bool createTestFileImpl(const string& file_name,
                        uint64_t count,
                        function<T(int)> factory)  // WARNING: this only works with 4 byte types
{
   // Open file
   ofstream of(file_name, ios::binary);
   if (!of.is_open() || !of.good())
      return false;

   // Write file in buffered fashion
   const uint32_t kMaxBufferSize = 1 << 22;
   vector<T> buffer(kMaxBufferSize / sizeof(uint32_t));
   for (uint64_t i = 0; i < count;) {
      // Fill buffer and write
      uint64_t limit = i + buffer.size();
      for (; i < count && i < limit; i++)
         buffer[i % buffer.size()] = factory(i);
      of.write(reinterpret_cast<char*>(buffer.data()), (buffer.size() - (limit - i)) * sizeof(uint32_t));
   }

   // Finish up
   of.flush();
   of.close();
   return of.good();
}
// -------------------------------------------------------------------------------------
template <class T>
bool foreachInFileImpl(const string& file_name, function<void(T)> callback)
{
   // Open file
   ifstream in(file_name, ios::binary);
   if (!in.is_open() || !in.good())
      return false;

   // Loop over each entry
   T entry;
   while (true) {
      in.read(reinterpret_cast<char*>(&entry), sizeof(uint32_t));
      if (!in.good())
         break;
      callback(entry);
   }
   return true;
}
// -------------------------------------------------------------------------------------
bool CreateTestFile(const string& file_name, uint64_t count, function<int32_t(int32_t)> factory)
{
   return createTestFileImpl<int32_t>(file_name, count, factory);
}
// -------------------------------------------------------------------------------------
bool ForeachInFile(const string& file_name, function<void(uint32_t)> callback)
{
   return foreachInFileImpl<uint32_t>(file_name, callback);
}
// -------------------------------------------------------------------------------------
bool CreateDirectory(const string& directory_name)
{
   return mkdir(directory_name.c_str(), 0666) == 0;
}
// -------------------------------------------------------------------------------------
bool CreateFile(const string& file_name, const uint64_t bytes)
{
   int file_fd = open(file_name.c_str(), O_CREAT | O_WRONLY, 0666);
   if (file_fd < 0) {
      return false;  // Use strerror(errno) to find error
   }

   if (ftruncate(file_fd, bytes) != 0) {
      return false;  // Use strerror(errno) to find error
   }

   if (close(file_fd) != 0) {
      return false;  // Use strerror(errno) to find error
   }

   return true;
}
// -------------------------------------------------------------------------------------
bool CreateFile(const string& file_name, const string& content)
{
   int file_fd = open(file_name.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666);
   if (file_fd < 0) {
      return false;  // Use strerror(errno) to find error
   }

   size_t written_bytes = write(file_fd, content.data(), content.size());
   return written_bytes == content.size();
}
// -------------------------------------------------------------------------------------
void DeleteFile(const std::string& file_name)
{
   remove(file_name.c_str());
}
// -------------------------------------------------------------------------------------
uint64_t GetFileLength(const string& file_name)
{
   int fileFD = open(file_name.c_str(), O_RDWR);
   if (fileFD < 0) {
      cout << "Unable to open file" << endl;  // You can posix_check errno to see what happend
      throw;
   }
   if (fcntl(fileFD, F_GETFL) == -1) {
      cout << "Unable to call fcntl on file" << endl;  // You can posix_check errno to see what happend
      throw;
   }
   struct stat st;
   fstat(fileFD, &st);
   close(fileFD);
   return st.st_size;
}
// -------------------------------------------------------------------------------------
bool fileExists(const string& file_name)
{
   struct stat buffer;
   bool exists = (stat(file_name.c_str(), &buffer) == 0);
   return exists && (buffer.st_mode & S_IFREG);
}
// -------------------------------------------------------------------------------------
bool directoryExists(const string& file_name)
{
   struct stat buffer;
   bool exists = (stat(file_name.c_str(), &buffer) == 0);
   return exists && (buffer.st_mode & S_IFDIR);
}
// -------------------------------------------------------------------------------------
bool pathExists(const string& file_name)
{
   struct stat buffer;
   bool exists = (stat(file_name.c_str(), &buffer) == 0);
   return exists;
}
// -------------------------------------------------------------------------------------
string LoadFileToMemory(const string& file_name)
{
   uint64_t length = GetFileLength(file_name);
   string data(length, 'a');
   ifstream in(file_name);
   in.read(&data[0], length);
   return data;
}
// -------------------------------------------------------------------------------------
namespace
{
// -------------------------------------------------------------------------------------
uint64_t applyPrecision(uint64_t input, uint32_t precision)
{
   uint32_t digits = log10(input) + 1;
   if (digits <= precision)
      return input;
   uint32_t invalidDigits = pow(10, digits - precision);
   return (uint64_t)((double)input / invalidDigits + .5f) * invalidDigits;
}
// -------------------------------------------------------------------------------------
}  // namespace
// -------------------------------------------------------------------------------------
string FormatTime(chrono::nanoseconds ns, uint32_t precision)
{
   ostringstream os;

   uint64_t timeSpan = applyPrecision(ns.count(), precision);

   // Convert to right unit
   if (timeSpan < 1000ll)
      os << timeSpan << "ns";
   else if (timeSpan < 1000ll * 1000ll)
      os << timeSpan / 1000.0f << "us";
   else if (timeSpan < 1000ll * 1000ll * 1000ll)
      os << timeSpan / 1000.0f / 1000.0f << "ms";
   else if (timeSpan < 60l * 1000ll * 1000ll * 1000ll)
      os << timeSpan / 1000.0f / 1000.0f / 1000.0f << "s";
   else if (timeSpan < 60l * 60l * 1000ll * 1000ll * 1000ll)
      os << timeSpan / 1000.0f / 1000.0f / 1000.0f / 60.0f << "m";
   else
      os << timeSpan / 1000.0f / 1000.0f / 1000.0f / 60.0f / 60.0f << "h";

   return os.str();
}
// -------------------------------------------------------------------------------------
void PinThread(int socket)
{
#ifdef __linux__
   // Doesn't work on OS X right now
   cpu_set_t cpuset;
   pthread_t thread;
   thread = pthread_self();
   CPU_ZERO(&cpuset);
   CPU_SET(socket, &cpuset);
   pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   int s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0) {
      fprintf(stderr, "error: pthread_getaffinity_np");
      exit(-1);
   }
#endif
   (void)socket;
}
// -------------------------------------------------------------------------------------
void RunMultithreaded(uint32_t thread_count, function<void(uint32_t)> foo)
{
   atomic<bool> start(false);
   vector<unique_ptr<thread>> threads(thread_count);
   for (uint32_t i = 0; i < thread_count; i++) {
      threads[i] = make_unique<thread>([i, &foo, &start]() {
         while (!start)
            ;
         foo(i);
      });
   }
   start = true;
   for (auto& iter : threads) {
      iter->join();
   }
}
// -------------------------------------------------------------------------------------
uint8_t* AlignedAlloc(uint64_t alignment, uint64_t size)
{
   void* result = nullptr;
   int error = posix_memalign(&result, alignment, size);
   if (error) {
      throw ex::GenericException("posix_memalign failed in utility");
   }
   return reinterpret_cast<uint8_t*>(result);
}
// -------------------------------------------------------------------------------------
namespace
{
array<char, 16> NUM_TO_HEX{{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}};
uint8_t HexToNum(char c)
{
   if ('a' <= c && c <= 'f') {
      return 10 + (c - 'a');
   }
   if ('0' <= c && c <= '9') {
      return (c - '0');
   }
   UNREACHABLE();
}
}  // namespace
// -------------------------------------------------------------------------------------
const string DataToHex(uint8_t* data, uint32_t len, bool spaces)
{
   string result;
   for (uint32_t i = 0; i < len; i++) {
      result += NUM_TO_HEX[*(data + i) >> 4];
      result += NUM_TO_HEX[*(data + i) & 0x0f];
      if (spaces && i != len - 1)
         result += ' ';
   }
   return result;
}
// -------------------------------------------------------------------------------------
const string StringToHex(const string& str, bool spaces)
{
   return DataToHex((uint8_t*)str.data(), str.size(), spaces);
}
// -------------------------------------------------------------------------------------
const vector<uint8_t> HexToData(const string& str, bool spaces)
{
   assert(spaces || str.size() % 2 == 0);

   uint32_t result_size = spaces ? ((str.size() + 1) / 3) : (str.size() / 2);
   vector<uint8_t> result(result_size);
   for (uint32_t i = 0, out = 0; i < str.size(); i += 2, out++) {
      result[out] = (HexToNum(str[i]) << 4) | HexToNum(str[i + 1]);
      i += spaces ? 1 : 0;
   }

   return result;
}
// -------------------------------------------------------------------------------------
const string HexToString(const string& str, bool spaces)
{
   assert(spaces || str.size() % 2 == 0);

   uint32_t result_size = spaces ? ((str.size() + 1) / 3) : (str.size() / 2);
   string result(result_size, 'x');
   for (uint32_t i = 0, out = 0; i < str.size(); i += 2, out++) {
      result[out] = (char)((HexToNum(str[i]) << 4) | HexToNum(str[i + 1]));
      i += spaces ? 1 : 0;
   }

   return result;
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
   // -------------------------------------------------------------------------------------