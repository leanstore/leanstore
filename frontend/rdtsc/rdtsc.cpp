// -------------------------------------------------------------------------------------
//
#include <emmintrin.h>
#include "Time.hpp"

#include <iostream>
#include <chrono>

std::tuple<s64, s64> meassureRdTsc(int seconds) {
   mean::TimePoint startChrono = mean::getTimePoint();
   s64 start = __rdtsc(); 
   _mm_sfence();

   while (mean::timePointDifference(mean::getTimePoint(), startChrono) < seconds*1000000000ull -60)
   {}

   _mm_lfence();
   s64 end = __rdtsc();
   mean::TimePoint endChrono = mean::getTimePoint();

   return {end - start, mean::timePointDifference(endChrono, startChrono)};
}

int main(int , char** ) {
   std::cout << "start" << std::endl;

   // calibrate
   auto cali = meassureRdTsc(10);
   auto tscPerNs = (double)(std::get<0>(cali)/*Hz*/) /  std::get<1>(cali) /*ns*/ ;

   std::cout << "calibration done" << std::endl;

   // meassurement
   auto mes = meassureRdTsc(20);
   auto tsc = std::get<0>(cali);
   auto chr = std::get<1>(cali);

   std::cout << "readTSC diff: " << tsc << " cycles" << std::endl;
   std::cout << "chrono diff: " << chr << " ns" << std::endl;
   std::cout << "readTSC diff converted: " << (s64)(tsc/tscPerNs) << " difference to chrono: " <<  tsc/tscPerNs -  (s64)chr << " ns" << std::endl;
   std::cout << "conversion rate used waitForSeconds1 TSC cycle = " << 1/(double)tscPerNs << " ns" << std::endl;

   return 0;
}
