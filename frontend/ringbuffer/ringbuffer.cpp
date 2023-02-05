// -------------------------------------------------------------------------------------
#include "leanstore/utils/RingBuffer.hpp"
#include "leanstore/utils/RingBufferSPSC.hpp"

#include <iostream>
#include <deque>
#include <random>

using namespace leanstore::utils;
int main(int , char** ) {
   std::cout << "start" << std::endl;
   
   std::deque<int> test;
   RingBuffer<int> ring(256);

   std::random_device rd;  //Will be used to obtain a seed for the random number engine
   std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
   std::uniform_int_distribution<> insupdist(0, 1000000);
   std::uniform_int_distribution<> valdist(0, 1000000);

   u64 fullcnt = 0;
   u64 emptycnt = 0;
   u64 inserted = 0;
   u64 popped = 0;
   // init
   for (int i = 0; i < (int)ring.max_size / 2; i++) {
      int val = valdist(gen);
      ring.push_back(val);
      test.push_back(val);
      inserted++;
   }
   // run
   for (u64 i = 0; i < 1e8; i++) {
      int insup = insupdist(gen);
      if (insup < 500700 && !ring.full()) {
         int val = valdist(gen);
         ring.push_back(val);
         test.push_back(val);
         inserted++;
         //std::cout << "push val" << std::endl;
      } else {
         int val;
         if (ring.try_pop(val)) {
            int tval = test.front();
            test.pop_front();
            if (val != tval) throw "";
            popped++;
            //std::cout << "pop val" << std::endl;
         }
      }
      if (ring.full()) {
         fullcnt++;
         if (inserted - popped != ring.max_size) throw "";
      }
      if (ring.empty()) {
         emptycnt++;
         if (inserted - popped != 0) throw "";
      }
#ifndef NDEBUG
      if (ring.inserted != inserted) throw "";
      if (ring.erased != popped) throw "";
#endif
      if (ring.contains != inserted - popped) throw "";
   }

   std::cout << "full: " << fullcnt << " empty: " << emptycnt << std::endl;
   return 0;


   for (int i = 0; i<10; i++) {
      ring.push_back(i+1);
   }

   int val;
   ring.try_pop(val); 
   ring.try_pop(val); 
   ring.try_pop(val); 
   std::cout << "pop1: "<< val << std::endl;
  
   ring.push_back(13);
   ring.push_back(13);
   std::cout << "emplace: "<< 13 << std::endl;

   while (ring.try_pop(val)) {
      std::cout << val << std::endl;
   }

   ring.try_pop(val);
   ring.try_pop(val);
   ring.try_pop(val);

   ring.push_back(14);

   while (ring.try_pop(val)) {
      std::cout << val << std::endl;
   }

   return 0;
}
