#pragma once

#include <iostream>

inline std::string getEnv(std::string var, std::string defaultValue) {
	char* bla = std::getenv(var.c_str());
	return bla ? std::string(bla) : defaultValue;
}
inline std::string getEnvRequired(std::string var) {
	char* bla = std::getenv(var.c_str());
   if (!bla) { 
      std::cerr << var << " env variable required" << std::endl;
      exit(-1);   
   }
	return std::string(bla);
}
inline float getEnv(std::string var, float defaultValue) {
	char* bla = std::getenv(var.c_str());
	return bla ? atof(bla) : defaultValue;
}
inline long getBytesFromString(std::string str) {
   // yeah very basic
   std::string::size_type pos;
   pos = str.find("K");
   if (pos != std::string::npos) {
      return std::atof(str.substr(0, pos).c_str())*1024;
   }
   pos = str.find("M");
   if (pos != std::string::npos) {
      return std::atof(str.substr(0, pos).c_str())*1024*1024;
   }
   pos = str.find("G");
   if (pos != std::string::npos) {
      return std::atof(str.substr(0, pos).c_str())*1024*1024*1024;
   }
   pos = str.find("T");
   if (pos != std::string::npos) {
      return std::atof(str.substr(0, pos).c_str())*1024*1024*1024*1024;
   }
   return std::atof(str.c_str());
}
