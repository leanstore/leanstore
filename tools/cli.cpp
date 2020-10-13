#include <termios.h>
#include <unistd.h>

#include <iostream>
using namespace std;
struct termios orig_termios;
void enableRawMode();
void disableRawMode();
void enableRawMode()
{
   tcgetattr(STDIN_FILENO, &orig_termios);
   atexit(disableRawMode);
   struct termios raw = orig_termios;
   raw.c_lflag &= ~(ECHO | ICANON);
   tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw);
}
void disableRawMode()
{
   tcsetattr(STDIN_FILENO, TCSAFLUSH, &orig_termios);
}
int main()
{
   {
      struct winsize w;
      ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
      printf("lines %d\n", w.ws_row);
      printf("columns %d\n", w.ws_col);
   }
   enableRawMode();
   char c;
   while (read(STDIN_FILENO, &c, 1) == 1 && c != 'q') {
      std::system("clear");
      if (iscntrl(c)) {
         printf("%d\n", c);
      } else {
         printf("%d ('%c')\n", c, c);
      }
   }
   int i = 0;
   disableRawMode();

   cout.flush();
   std::system("clear");
   std::system("stty raw -echo");
   while (true) {
      cout << "Hello World" << i << endl;
      sleep(1);
      cout.flush();
      std::system("clear");
      i++;
      char ch = getchar();
      if (ch == 'h') {
         cout << "help" << endl;
      }
   }
   std::system("stty cooked echo");
   return 0;
}
