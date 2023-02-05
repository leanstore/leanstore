// -------------------------------------------------------------------------------------
#include "leanstore/utils/SmallList.hpp"

#include <iostream>

using namespace leanstore::utils;
template <typename TL>
void printList(TL& list) {
   std::cout << "list size: " << list.size() << " {" << std::flush;
   for (int i: list) {
      std::cout << i << ", " << std::flush;
   }
   std::cout << "}" << std::endl;
}
int main(int , char** ) {
   std::cout << "start" << std::endl;
   
   int n = 10;
   SmallForwardList<int> list(n);
   for (int i = 0; i < n; i++) {
      list.emplace_back(i+1);
   }

   printList(list);

   std::cout << "delete last" << std::endl;
   auto it3 = std::next(list.begin(), n-2);
   list.erase_after(it3);
   
   printList(list);
  
   std::cout << "delete middle" << std::endl;
   auto it2 = std::next(list.begin(), list.size() / 2 -1);
   list.erase_after(it2);

   printList(list);
   
   std::cout << "add a few" << std::endl;
   list.emplace_back(1000);


   printList(list);

   std::cout << "delete all" << std::endl;  
   auto it1 = list.before_begin();
   while (std::next(it1, 1) != list.end()) {
      auto it_after = std::next(it1, 1); 
      std::cout << "delete: " << *it_after << std::endl;
      list.erase_after(it1);
      printList(list);
   }

   printList(list);

   auto it = list.begin();
   if (it != list.end()) {
      assert(false);
   }

   return 0;
}
