// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef GLOBAL_H
#define GLOBAL_H

#include <string>

#ifndef TEST
const int DEFAULT_CHUNK_SIZE = 512;
const int MAX_PACKET_SIZE = 1000 * 1024; // TODO : find a good value
const int MAX_OUTPUT_PACKETS = 100; // in packets
#else
const int DEFAULT_CHUNK_SIZE = 4;
const int MAX_PACKET_SIZE = 100; // TODO : find a good value
const int MAX_OUTPUT_PACKETS = 5; // in packets
#endif

namespace global {

template<class T>
inline int getType() {
  return 0;
}

template<>
inline int getType<int>() {
  return 1;
}

template<>
inline int getType<double>() {
  return 2;
}

template<>
inline int getType<char>() {
  return 3;
}


const int TypeSize[] = { 0, 4, 8, 1 };


template<class T>
inline std::string getTypeName() {
  return "(unknown)";
}

template<>
inline std::string getTypeName<int>() {
  return "INT";
}

template<>
inline std::string getTypeName<double>() {
  return "DOUBLE";
}

template<>
inline std::string getTypeName<char>() {
  return "BOOL";
}

}

#endif

