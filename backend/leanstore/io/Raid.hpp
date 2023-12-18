#pragma once

#include <cassert>
#include <cstdint>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

class Raid0
{
   const int deviceCnt;
   const int chunkSize;
  public:
   Raid0(int deviceCnt, int chunkSize) : deviceCnt(deviceCnt), chunkSize(chunkSize) {}
   void calc(uint64_t offset, int& deviceOut, uint64_t& offsetOut)
   {
      const uint64_t chunk = offset / chunkSize;
      const uint64_t offsetInChunk = offset % chunkSize;
      deviceOut = chunk % deviceCnt;
      offsetOut = (chunk / deviceCnt) * chunkSize + offsetInChunk;  // / deviceCnt * lbasPerDevice + addrRemainder;
   }
};

class Raid5
{
   const int deviceCnt;
   const int chunkSize;
  public:
   Raid5(int deviceCnt, int chunkSize) : deviceCnt(deviceCnt), chunkSize(chunkSize) {}
   void calc(uint64_t offset, int& deviceOut, uint64_t& offsetOut)
   {
      const uint64_t chunk = offset / chunkSize;
      const uint64_t offsetInChunk = offset % chunkSize;
      deviceOut = chunk % deviceCnt;
      offsetOut = (chunk / deviceCnt) * chunkSize + offsetInChunk;  // / deviceCnt * lbasPerDevice + addrRemainder;
   }
};

template<typename DeviceType>
class RaidController
{
   static constexpr int CHUNK_SIZE = 64 * 1024;
   std::vector<std::string> devices;
   std::vector<DeviceType> fds;
  public:
   RaidController(std::string connectionString)
   {
      std::istringstream iss(connectionString);
      std::string device;
      while (std::getline(iss, device, ';')) {
         devices.push_back(device);
      }
      fds.resize(devices.size());
   }
   int deviceCount() {
      return fds.size();
   }
   DeviceType device(int d) {
      return fds.at(d);
   }
   std::string name(int d) {
      return devices.at(d);
   }
   DeviceType deviceTypeOrFd(int d) {
      return fds.at(d);
   }
   void forEach(std::function<void (std::string& dev, DeviceType& fd)> fun)
   {
      const int size = devices.size();
      for (int i = 0; i < size; i++) {
         fun(devices[i], fds[i]);
      }
   }
   // this functions assumes the request is smaller than CHUNK_SIZE,
   // so use carefully
   /*
   std::tuple<DeviceType& deviceOut, uint64_t offsetOut> calc(uint64_t offset, uint64_t len) {
      assert(len <= CHUNK_SIZE);
      Raid0 raid(devices.size(), CHUNK_SIZE);
      uint64_t offsetOut;
      int deviceSelector;
      raid.calc(offset, deviceSelector, offsetOut);
      deviceOut = &fds[deviceSelector];
      return std::make_tuple(fds[deviceSelector], offsetOut);
   }*/
   void calc(uint64_t offset, uint64_t len, DeviceType*& deviceOut, uint64_t& offsetOut)
   {
      assert(len <= CHUNK_SIZE);
      Raid0 raid(devices.size(), CHUNK_SIZE);
      int deviceSelector;
      raid.calc(offset, deviceSelector, offsetOut);
      deviceOut = &fds[deviceSelector];
   }
   /*
   using SubmissionFun = std::function<void(char*, DeviceType&, uint64_t, uint64_t)>;
   void submitIos(char* buf, uint64_t offset, uint64_t len, SubmissionFun submission)
   {
      Raid0 raid(devices.size(), CHUNK_SIZE);
      int deviceSelector;
      uint64_t offsetOut;
      uint64_t remaining = len;
      do {
         raid.calc(offset, deviceSelector, offsetOut);
         submission(buf, fds[deviceSelector], offsetOut, remaining > CHUNK_SIZE ? CHUNK_SIZE : remaining);
         remaining -= CHUNK_SIZE;
         offset += CHUNK_SIZE;
         buf += CHUNK_SIZE;
         // remaining
      } while (remaining > 0);
   }
   */
};
