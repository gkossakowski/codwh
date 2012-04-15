// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef COLUMN_H
#define COLUMN_H

#include <cassert>
#include "factory.h"
#include "global.h"

class Column {
 public:
  int size;
  virtual void consume(int column_index, Server* server) = 0;
};

template<class T>
class ColumnChunk : public Column {
 public:
  T chunk[DEFAULT_CHUNK_SIZE];
  void consume(int column_index, Server* server);
};

template<>
inline void
ColumnChunk<int>::consume(int column_index, Server* server) {
  server->ConsumeInts(column_index, size, &chunk[0]);
}

template<>
inline void
ColumnChunk<double>::consume(int column_index, Server* server) {
  server->ConsumeDoubles(column_index, size, &chunk[0]);
}

template<>
inline void
ColumnChunk<char>::consume(int column_index, Server* server) {
  server->ConsumeBitBools(column_index, size, &chunk[0]);
}

class ColumnProvider : public Node {
  public:
    ColumnProvider(int id):columnIndex(id) {}
    int columnIndex;
    virtual Column* pull() = 0;
};

template<class T>
class ColumnProviderImpl : public ColumnProvider {
  ColumnChunk<T> columnCache;
 public:
  ColumnProviderImpl(int id): ColumnProvider(id) {}

  inline Column* pull();

  std::ostream& debugPrint(std::ostream& out) {
    return out << "ColumnProvider(" << columnIndex << ")";
  }
};

template<>
inline Column*
ColumnProviderImpl<int>::pull() {
  columnCache.size =
    Factory::server->GetInts(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}

template<>
inline Column*
ColumnProviderImpl<double>::pull() {
  columnCache.size =
    Factory::server->GetDoubles(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}

template<>
inline Column*
ColumnProviderImpl<char>::pull() {
  columnCache.size =
    Factory::server->GetBitBools(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}


#endif

