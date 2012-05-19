// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef COLUMN_H
#define COLUMN_H

#include <cassert>

#include "filter.h"
#include "factory.h"
#include "global.h"
#include <string>
#include <sstream>

union any_t {
  int int32;
  double double_;
  bool boolean;
};

template <typename T>
std::string to_string(T const& value) {
  std::stringstream sstr;
  sstr << value;
  return sstr.str();
}

class Column {
 public:
  int size;
  virtual void consume(int column_index, Server* server) = 0;
  virtual void filter(Column* cond, Column* result) = 0;
  virtual void fill(any_t* any, int idx) = 0;
  virtual void addTo(any_t* any, int idx) = 0;
  virtual void add(Column* col, int src_idx, int target_idx) = 0;
  virtual void take(const any_t& any, int idx) = 0;
  virtual Column* clone() = 0;
  virtual void debugPrint() = 0;
  virtual void zero(int idx) = 0;
};

template<class T>
class ColumnChunk : public Column {
 public:
  T chunk[DEFAULT_CHUNK_SIZE];
  void consume(int column_index, Server* server);
  void fill(any_t* any, int idx);
  void addTo(any_t* any, int idx);
  void take(const any_t& any, int idx);
  void zero(int idx);
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
  Column* clone() {
    ColumnChunk* result = new ColumnChunk;
    result->size = size;
    memcpy(result->chunk, chunk, sizeof(T)*DEFAULT_CHUNK_SIZE);
    return result;
  }
  void add(Column* col, int src_idx, int target_idx) {
    printf("add called with T = %s\n", typeid(T).name());
    ColumnChunk<T>* src = static_cast<ColumnChunk<T>*>(col);
    // TODO what about adding bools?
    T* src_chunk = src->chunk;
    T src_value = src_chunk[src_idx];
    std::string src_value_str = to_string(src_value);
    printf("value = %s\n" , src_value_str.c_str());
    chunk[target_idx] += src_value;
  }
  void debugPrint() {
    printf("ColumnChunk::debugPrint\n");
    printf("T = %s\n", typeid(T).name());
    printf("size = %d\n", size);
    printf("chunk = ");
    for (int i=0; i<size; i++) {
      printf("%s, ", to_string(chunk[i]).c_str());
    }
    printf("\n");
  }
};

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
ColumnChunk<int>::zero(int idx) {
  printf("<int> zero(%d)\n", idx);
  chunk[idx] = 0;
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
ColumnChunk<double>::zero(int idx) {
  printf("<double> zero(%d)\n", idx);
  chunk[idx] = 0.0;
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
ColumnChunk<char>::zero(int idx) {
  printf("<char> zero(%d)\n", idx);
  // false
  chunk[idx / 8] &= ~(1 << (idx & 7));
}

// }}}

template<>
inline void
ColumnChunk<char>::add(Column* col, int src_idx, int target_idx) {
  assert(false);
  //  ColumnChunk<T>* src = static_cast<ColumnChunk<T>*>(src);
  // TODO what about adding bools?
  //  chunk[target_idx] += src->chunk[src_idx];
}

class ColumnProvider : public Node {
  public:
    virtual Column* pull() = 0;
    virtual int getType() = 0;
};

template<class T>
class ColumnProviderImpl : public ColumnProvider {
  ColumnChunk<T> columnCache;
  int columnIndex;
 public:
  ColumnProviderImpl(int id): columnIndex(id) {}

  inline Column* pull();

  int getType() {
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

