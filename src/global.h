// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef GLOBAL_H
#define GLOBAL_H

#include <string>
#include "proto/operations.pb.h"

#ifndef TEST_FLAG
const int DEFAULT_CHUNK_SIZE = 512; // in rows
const int MAX_PACKET_SIZE = 1000 * 1024; // (in bytes) TODO : find a good value
const int MAX_OUTPUT_PACKETS = 100; // in packets
#else
const int DEFAULT_CHUNK_SIZE = 5;
const int MAX_PACKET_SIZE = 100;
const int MAX_OUTPUT_PACKETS = 15;
#endif

namespace global {

template<class T>
inline query::ColumnType getType() {
  return query::INVALID_TYPE;
}

template<>
inline query::ColumnType getType<int>() {
  return query::INT;
}

template<>
inline query::ColumnType getType<double>() {
  return query::DOUBLE;
}

template<>
inline query::ColumnType getType<char>() {
  return query::BOOL;
}

template<>
inline query::ColumnType getType<size_t>() {
  return query::HASH;
}

inline int getTypeSize(query::ColumnType type_) {
  switch (type_) {
    case query::INT:
      return 4;
    case query::DOUBLE:
      return 8;
    case query::BOOL:
      return 1;
    case query::HASH:
      return sizeof(size_t);
    default:
      assert (false);
      return 0;
  }
}

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

template<>
inline std::string getTypeName<size_t>() {
  return "HASH";
}

}

#endif

