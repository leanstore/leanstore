#include <fcntl.h>
#include <tbb/tbb.h>
#include <unistd.h>
#include <cassert>
#include <iostream>
#include "/opt/PerfEvent.hpp"

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
int main(int argc, char** argv)
{
  PerfEvent e;
  tbb::task_scheduler_init taskScheduler(20);
  int fd = open("/dev/nvme2n1p1", O_RDWR | O_DIRECT | O_CREAT, 0666);
  {
    PerfEventBlock b(e, 1024 * 10);
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, 1024 * 10), [&](const tbb::blocked_range<uint64_t>& range) {
      vector<uint64_t> buffer(1024);
      for (uint64_t i = range.begin(); i < range.end(); i++) {
        pread(fd, buffer.data(), 1024, i);
        cout << buffer[10] << endl;
      }
    });
  }
  return 0;
}