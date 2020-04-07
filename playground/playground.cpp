#include <fcntl.h>
#include <tbb/tbb.h>
#include <unistd.h>

#include <PerfEvent.hpp>"
#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>

#include "Units.hpp"

using namespace std;
void callback(char* payload, uint8_t command)
{
  cout << *payload << endl;
  cout << int(command) << endl;
}

using SwizzlingCallback = void (*)(char* payload, uint8_t type);

class A
{
 public:
  int x;
  A(int x) : x(x) {}
  A& operator=(A&& b)
  {
    cout << "x = " << x << " ; ";
    cout << "move assignemnt" << endl;
    return *this;
  }
  A& operator=(A& b)
  {
    cout << "x = " << x << " ; ";
    cout << "copy assignemnt" << endl;
    return *this;
  }
  ~A()
  {
    cout << "x = " << x << " ; ";
    cout << "destruct A " << endl;
  }
};

template <typename T>
class IA
{
 protected:
  int x = 10, y;
};
template <typename T>
class IB : public IA<T>
{
 public:
  IB(int tata)
  {
    cout << tata << endl;
    cout << IA<T>::x << endl;
  }
};

struct Tata {
  Tata()
  {
    cout << "contructor" << endl;
    throw exception();
  }
  ~Tata() { cout << "decontructor" << endl; }
};
struct BFT {
  int counter;
} test_object;

int main(int argc, char** argv)
{
  cout << test_object.counter << endl;
  std::vector<std::thread> threads;
  std::atomic<u64> counter = 0;
  for (u32 i = 0; i < atoi(getenv("N")); i++) {
    threads.emplace_back([&]() {
      while (true) {
        counter++;
      }
    });
  }
  threads.emplace_back([&]() {
    while (true) {
      cout << counter.exchange(0) << endl;
      sleep(1);
    }
  });
  for (auto& thread : threads) {
    thread.join();
  }
  return 0;
}
