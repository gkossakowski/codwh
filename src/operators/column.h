// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef COLUMN_H
#define COLUMN_H

#include <cassert>
#include <cmath>
#include <typeinfo>

#include "filter.h"
#include "factory.h"
#include "global.h"
#include "node_environment/node_environment.h"
#include <boost/functional/hash.hpp>

union any_t {
  int int32;
  double double_;
  bool boolean;
  size_t hash;
};

class Column {
 public:
  Column(): size(0) { }
  int size;
  virtual query::ColumnType getType() = 0;
  virtual size_t transfuse(void *dst) = 0;
  virtual void consume(int column_index, Server* server) = 0;
  virtual void filter(Column* cond, Column* result) = 0;
  virtual void fill(any_t* any, int idx) = 0;
  virtual void addTo(any_t* any, int idx) = 0;
  virtual void take(const any_t& any, int idx) = 0;
  // zero the contents of the column
  virtual void zero() = 0;
  virtual void hash(Column* into) = 0;
};

template<class T>
class ColumnChunk : public Column {
 public:
  T chunk[DEFAULT_CHUNK_SIZE];
  query::ColumnType getType();
  size_t transfuse(void *dst);
  void consume(int column_index, Server* server);
  void fill(any_t* any, int idx);
  void addTo(any_t* any, int idx);
  void take(const any_t& any, int idx);
  void zero();
  void hash(Column* into) {
    assert(into->getType() == query::HASH);
    ColumnChunk<size_t>* intoCasted = static_cast<ColumnChunk<size_t>*>(into);
    for (int i = 0; i < size; i++) {
      boost::hash_combine(intoCasted->chunk[i], chunk[i]);
    }
  }
  void filter(Column* cond, Column* res) {
    ColumnChunk<char>* condition = static_cast<ColumnChunk<char>*>(cond);
    ColumnChunk<T>* result = static_cast<ColumnChunk<T>*>(res);

    unsigned char* cT = (unsigned char*) condition->chunk;
    T* target = result->chunk;

    int rest = 0;
    for (int i = 0 ; i < size ; i += 8) {
      rest += filterPrimitive(target, chunk + i, cT[i / 8], rest);
    }

    result->size = rest;
  }
};

// typeSize, transfuse {{{

template<class T>
inline query::ColumnType
ColumnChunk<T>::getType(){
  return global::getType<T>();
}

template<class T>
inline size_t
ColumnChunk<T>::transfuse(void *dst) {
  std::copy(chunk, chunk + size, reinterpret_cast<T*>(dst));
  return size * sizeof(T);
}

// }}}

// consume, fill, addto, take {{{

template<>
inline void
ColumnChunk<int>::consume(int column_index, Server* server) {
  server->ConsumeInts(column_index, size, &chunk[0]);
}

template<>
inline void
ColumnChunk<int>::fill(any_t* any, int idx) {
  any->int32 = chunk[idx];
}

template<>
inline void
ColumnChunk<int>::addTo(any_t* any, int idx) {
  any->int32 += chunk[idx];
}

template<>
inline void
ColumnChunk<int>::take(const any_t& any, int idx) {
  chunk[idx] = any.int32;
}

template<>
inline void
ColumnChunk<int>::zero() {
  for (int i = 0; i < size; i++) {
    chunk[i] = 0;
  }
}

template<>
inline void
ColumnChunk<double>::consume(int column_index, Server* server) {
  server->ConsumeDoubles(column_index, size, &chunk[0]);
}

template<>
inline void
ColumnChunk<double>::fill(any_t* any, int idx) {
  any->double_ = chunk[idx];
}

template<>
inline void
ColumnChunk<double>::addTo(any_t* any, int idx) {
  any->double_ += chunk[idx];
}

template<>
inline void
ColumnChunk<double>::take(const any_t& any, int idx) {
  chunk[idx] = any.double_;
}

template<>
inline void
ColumnChunk<double>::zero() {
  for (int i = 0; i < size; i++) {
    chunk[i] = 0.0;
  }
}

template<>
inline void
ColumnChunk<char>::consume(int column_index, Server* server) {
  server->ConsumeBitBools(column_index, size, &chunk[0]);
}

template<>
inline void
ColumnChunk<char>::fill(any_t* any, int idx) {
  any->boolean = ((chunk[idx / 8] & (1 << (idx & 7))) != 0);
}

template<>
inline void
ColumnChunk<char>::addTo(any_t* any, int idx) {
  if (chunk[idx / 8] & (1 << (idx &7))) {
    any->int32 += 1;
  }
}

template<>
inline void
ColumnChunk<char>::take(const any_t& any, int idx) {
  if (any.boolean) {
    chunk[idx / 8] |= 1 << (idx & 7); 
  } else {
    chunk[idx / 8] &= ~(1 << (idx & 7)); 
  }
}

template<>
inline void
ColumnChunk<char>::zero() {
  for (int i = 0; i < size/8; i++) {
    chunk[i] = 0;
  }
}

template<>
inline void
ColumnChunk<size_t>::consume(int column_index, Server* server) {
  assert(false);
}

template<>
inline void
ColumnChunk<size_t>::fill(any_t* any, int idx) {
  any->hash = chunk[idx];
}

template<>
inline void
ColumnChunk<size_t>::addTo(any_t* any, int idx) {
  any->hash += chunk[idx];
}

template<>
inline void
ColumnChunk<size_t>::take(const any_t& any, int idx) {
  chunk[idx] = any.hash;
}

template<>
inline void
ColumnChunk<size_t>::zero() {
  for (int i = 0; i < size; i++) {
    chunk[i] = 0;
  }
}
// }}}

class ColumnProvider : public Node {
  public:
    virtual Column* pull() = 0;
    virtual query::ColumnType getType() = 0;
};

// ColumnProviderServer {{{
template<class T>
class ColumnProviderServer : public ColumnProvider {
  ColumnChunk<T> columnCache;
  int columnIndex;
 public:
  ColumnProviderServer(int id): columnIndex(id) {}

  inline Column* pull();

  query::ColumnType getType() {
    return global::getType<T>();
  }

  std::ostream& debugPrint(std::ostream& out) {
    return out << "ColumnProvider(" << columnIndex << ": "
        << global::getTypeName<T>() << ")";
  }
};

template<>
inline Column*
ColumnProviderServer<int>::pull() {
  columnCache.size =
    Factory::server->GetInts(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}

template<>
inline Column*
ColumnProviderServer<double>::pull() {
  columnCache.size =
    Factory::server->GetDoubles(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}

template<>
inline Column*
ColumnProviderServer<char>::pull() {
  columnCache.size =
    Factory::server->GetBitBools(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}
// }}}

template <class T>
inline ColumnChunk<T>*
deserializeChunk(int from_row, const char bytes[], size_t rows) {
  //printf("deserializeChunk...\n");
  ColumnChunk<T> *col = new ColumnChunk<T>();
  const T* values = reinterpret_cast<const T*>(bytes);
  assert(rows <= (size_t) DEFAULT_CHUNK_SIZE);
  std::copy(values + from_row, values + from_row + rows, col->chunk);
  col->size = rows;
  return col;
}

// ColumnProviderFile {{{
template<class T>
class ColumnProviderFile : public ColumnProvider {
  ColumnChunk<T> columnCache;
  DataSourceInterface** source;
  int columnIndex;
 public:
  ColumnProviderFile(DataSourceInterface** source_, int id):
    source(source_), columnIndex(id) {}

  inline Column* pull();

  query::ColumnType getType() {
    return global::getType<T>();
  }

  std::ostream& debugPrint(std::ostream& out) {
    return out << "ColumnProviderFile(" << columnIndex << ": "
        << global::getTypeName<T>() << ")";
  }
};

template<>
inline Column*
ColumnProviderFile<int>::pull() {
  columnCache.size =
      (*source)->GetInts(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}

template<>
inline Column*
ColumnProviderFile<double>::pull() {
  columnCache.size =
      (*source)->GetDoubles(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}

template<>
inline Column*
ColumnProviderFile<char>::pull() {
  columnCache.size =
      (*source)->GetBitBools(columnIndex, DEFAULT_CHUNK_SIZE, &columnCache.chunk[0]);
  return &columnCache;
}
// }}}


#endif

