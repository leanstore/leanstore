#include <iostream>
using namespace std;
void callback(char *payload, uint8_t command) {
   cout << *payload << endl;
   cout << int(command) << endl;

}

using SwizzlingCallback = void (*)(char *payload, uint8_t type);

int main(int argc, char **argv) {
   SwizzlingCallback tata = &callback;
   char *text ="Hello WOrld";
   tata(text, 10);
   return 0;
}