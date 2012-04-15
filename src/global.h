// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef GLOBAL_H
#define GLOBAL_H

const int DEFAULT_CHUNK_SIZE = 512;

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

}

#endif

