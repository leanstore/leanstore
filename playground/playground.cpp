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
int main(int argc, char **argv) {
   A a(1), b(2);
   A c(3);
   c = a;
   c = std::move(a);
   c = std::move(a);
   return 0;
}