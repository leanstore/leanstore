#include <iostream>
using namespace std;
void callback(char *payload, uint8_t command) {
   cout << *payload << endl;
   cout << int(command) << endl;

}

using SwizzlingCallback = void (*)(char *payload, uint8_t type);

class A {
public:
   int x;
   A(int x) :x(x){

   }
   A& operator=(A&& b) {
      cout << "x = " << x <<" ; ";
      cout << "move assignemnt" << endl;
      return *this;
   }
   A& operator=(A& b) {
      cout << "x = " << x <<" ; ";
      cout <<"copy assignemnt" << endl;
      return *this;
   }
   ~A() {
      cout << "x = " << x <<" ; ";
      cout <<"destruct A " << endl;
   }

};

template<typename T>
class IA {
protected:
   int x = 10,y;
};
template<typename T>
class IB: public  IA<T> {
public:
   IB(int tata) {
      cout << tata << endl;
      cout << IA<T>::x << endl;
   }
};
int main(int argc, char **argv) {
//   A a(1), b(2);
//   A c(3);
//   c = a;
//   c = std::move(a);
//   c = std::move(a);

   IB<char> ib(20);
   return 0;
}