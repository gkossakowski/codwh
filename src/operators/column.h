// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef COLUMN_H
#define COLUMN_H

#include <cassert>

#include "filter.h"
#include "factory.h"
#include "global.h"

union any_t {
  int int32;
  double double_;
  bool boolean;
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
// }}}

class ColumnProvider : public Node {
  public:
    virtual Column* pull() = 0;
    virtual query::ColumnType getType() = 0;
};

template<class T>
class ColumnProviderImpl : public ColumnProvider {
  ColumnChunk<T> columnCache;
  int columnIndex;
 public:
  ColumnProviderImpl(int id): columnIndex(id) {}

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

