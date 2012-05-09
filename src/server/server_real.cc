// Copyright 2012 Google Inc. All Rights Reserved.
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <vector>
#include <utility>
#include <string>
#include <cstring>
#include <sys/time.h>
#include "server.h"

using ::std::min;
using ::std::vector;

namespace {

int random_power_of_two() {
  return 1 << (rand() % 31);
}

int random(int lo, int hi) {
  assert(hi >= lo);
  return lo + rand() % (hi - lo + 1);
}

// Returns a U(0, 1) variable.
double uniform() {
  return (double) (1 + (rand() % ((1 << 30) - 1))) / (double) (1 << 30);
}

// Returns a N(0, 1) variable using the Box-Muller method.
double normal() {
  return sqrt(-2. * log(uniform())) * cos(8 * atan(1) * uniform());
}

}  // namespace

// Stores generated data
template<class T>
class ColumnCache {
 public:
  ColumnCache() {
    array = NULL;
  }

  void Init(int query_size) {
    assert(array == NULL);
    array = new T[query_size];
    pos = 0;
    array_size = query_size;
  }

  void Copy(T* destination, int number) {
    memcpy(destination, array + pos, number * sizeof(*destination));
    pos += number;
  }

  T& operator[](int id) {
    return array[id];
  }

  ~ColumnCache() {
    if (array != NULL) {
      delete[] array;
    }
  }

 private:
  T* array;
  int pos;
  int array_size;
};

// A class serving data from a single column.
class ColumnServer {
 public:
  ColumnServer(int query_size) : rows_left(query_size) {}

  virtual int GetDoubles(int number, double* destination) {
    assert(false);
  }
  virtual int GetInts(int number, int32* destination) {
    assert(false);
  }
  virtual int GetByteBools(int number, bool* destination) {
    assert(false);
  }
  virtual int GetBitBools(int number, char* destination) {
    assert(false);
  }

 protected:
  int Serve(int N) {
    if (N >= rows_left) {
      N = rows_left;
    }
    rows_left -= N;
    return N;
  }

  int rows_left;
};

class RealDataServer : public Server {
 public:
  // The column_types vector should
  // contain types of columns - if column_types[i] = j, then the i-th
  // (0-indexed) column of the input is of type j (where 1 means int, 2 means
  // double and 3 means bool).
  RealDataServer(const vector<int> &column_types);
  ~RealDataServer();
  int GetDoubles(int column_index, int number, double* destination);
  int GetInts(int column_index, int number, int32* destination);
  int GetByteBools(int column_index, int number, bool* destination);
  int GetBitBools(int column_index, int number, char* destination);

  // The Consume methods printf the output to screen. This is useful for
  // correctness checking, for running benchmarks you should likely redefine
  // them to do nothing.
  void ConsumeDoubles(int column_index, int number, const double* destination);
  void ConsumeInts(int column_index, int number, const int32* destination);
  void ConsumeByteBools(int column_index, int number, const bool* destination);
  void ConsumeBitBools(int column_index, int number, const char* destination);

 private:
  vector<ColumnServer *> column_servers_;
};

class DoubleColumnServer : public ColumnServer {
 public:
  DoubleColumnServer(int query_size)
      : ColumnServer(query_size),
        low_range_(-2000 * random_power_of_two() % (1 << 30)),
        high_range_(low_range_ + random_power_of_two() % (1 << 30)),
        zoom_range_(random_power_of_two()),
        normal_(random(0, 1)) {
    cache.Init(query_size);
    for (int i = 0; i < query_size; ++i) {
      cache[i] = Generate();
    }
  }

  int GetDoubles(int number, double *destination) {
    //printf("SERVING %d to %d, zoom %d, normalcy %s\n",
    //       low_range_, high_range_, zoom_range_, normal_ ? "TRUE" : "FALSE");
    number = Serve(number);
    cache.Copy(destination, number);
    return number;
  }

 private:
  ColumnCache<double> cache;
  double Generate() {
    if (!normal_) {
      return (double) random(low_range_, high_range_) /
             (double) random(1, zoom_range_);
    } else {
      return (low_range_ + high_range_) / 2. +
             normal() * (high_range_ - low_range_) / (2. * zoom_range_);
    }
  }

  const int low_range_;
  const int high_range_;
  const int zoom_range_;
  const bool normal_;
};

// Column Server implementations.
class IntColumnServer : public ColumnServer {
 public:
  IntColumnServer(int query_size)
      : ColumnServer(query_size),
        low_range_(random_power_of_two() % (1 << 30)),
        high_range_(low_range_ + min(random_power_of_two(), 1 << 30)),
        normal_(random(0, 1)) {
    cache.Init(query_size);
    for (int i = 0; i < query_size; ++i) {
      cache[i] = Generate();
    }
  }

  int GetInts(int number, int *destination) {
    //printf("SERVING %d to %d, normalcy %s\n",
    //       low_range_, high_range_, normal_ ? "TRUE" : "FALSE");
    number = Serve(number);
    cache.Copy(destination, number);
    return number;
  }

 private:
  ColumnCache<int> cache;
  double Generate() {
    if (!normal_) {
      return random(low_range_, high_range_);
    } else {
      return (low_range_ + high_range_) / 2. +
             normal() * (high_range_ - low_range_) / 2.;
    }
  }

  const int low_range_;
  const int high_range_;
  const bool normal_;
};

class BoolColumnServer : public ColumnServer {
 public:
  BoolColumnServer(int query_size)
      : ColumnServer(query_size),
        probability_(random(0, 100)) {
    cache.Init((query_size + 7) / 8);
    for (int i = 0; i < query_size; ++i) {
      if (i % 8 == 0)
        cache[i / 8] = 0;
      cache[i / 8] |= (Generate() << (i & 7));
    }
  }

  int GetByteBools(int number, bool *destination) {
    //printf("SERVING probability %d\n", probability_);
    number = Serve(number);
    for (int i = 0; i < number; ++i) {
      destination[i] = Generate();
    }
    return number;
  }

  virtual int GetBitBools(int number, char* destination) {
    //printf("SERVING probability %d\n", probability_);
    number = Serve(number);
    cache.Copy(destination, (number + 7) / 8);
    return number;
  }

 private:
  ColumnCache<char> cache;
  char Generate() {
    return (random(0, 99) < probability_);
  }

  const int probability_;
};

// RealDataServer implementation.
RealDataServer::RealDataServer(const vector<int> &column_types) {
  //int query_size = random(1000000, 10000000);
  int query_size = 10;
  vector<int>::const_iterator it;
  for (it = column_types.begin(); it != column_types.end(); ++it) {
    switch(*it) {
      case 1: column_servers_.push_back(new IntColumnServer(query_size));
              break;
      case 2: column_servers_.push_back(new DoubleColumnServer(query_size));
              break;
      case 3: column_servers_.push_back(new BoolColumnServer(query_size));
              break;
      default: assert(false);
    }
  }
}

RealDataServer::~RealDataServer() {
  vector<ColumnServer *>::iterator it;
  for (it = column_servers_.begin(); it != column_servers_.end(); ++it) {
    delete *it;
  }
}

int RealDataServer::GetDoubles(int c, int n, double* d) {
  double res = column_servers_[c]->GetDoubles(n, d);
  printf("Generating doubles:\n");
  ConsumeDoubles(c, res, d);
  printf("End generating:\n");
  return res;
}

int RealDataServer::GetInts(int c, int n, int32* d) {
  int res = column_servers_[c]->GetInts(n, d);
  printf("Generating int:\n");
  ConsumeInts(c, res, d);
  printf("End generating:\n");
  return res;
}

int RealDataServer::GetByteBools(int c, int n, bool* d) {
  return column_servers_[c]->GetByteBools(n, d);
}

int RealDataServer::GetBitBools(int c, int n, char* d) {
  return column_servers_[c]->GetBitBools(n, d);
}

void RealDataServer::ConsumeDoubles(int column_index, int number,
                                  const double* d) {
  for (int i = 0; i < number; ++i) {
    printf("C%d: %f\n", column_index, d[i]);
  }
}

void RealDataServer::ConsumeInts(int column_index, int number, const int32* d) {
  for (int i = 0; i < number; ++i) {
    printf("C%d: %d\n", column_index, d[i]);
  }
}

void RealDataServer::ConsumeByteBools(int column_index, int number,
                                    const bool* d) {
  for (int i = 0; i < number; ++i) {
    printf("C%d: %s\n", column_index, d[i] ? "TRUE" : "FALSE");
  }
}

void RealDataServer::ConsumeBitBools(int column_index, int number,
                                   const char* d) {
  int pos = 0;
  char mask = 1;
  for (int i = 0; i < number; ++i) {
    if (!mask) {
      mask = 1;
      pos += 1;
    }
    printf("C%d: %s\n", column_index, (d[pos] & mask) ? "TRUE" : "FALSE");
    mask <<= 1;
  }
}

// TestDataServer

class TestDataServer : public Server {
public:
  // The column_types vector should
  // contain types of columns - if column_types[i] = j, then the i-th
  // (0-indexed) column of the input is of type j (where 1 means int, 2 means
  // double and 3 means bool).
  TestDataServer(const vector<int> &column_types);
  ~TestDataServer();
  int GetDoubles(int column_index, int number, double* destination);
  int GetInts(int column_index, int number, int32* destination);
  int GetByteBools(int column_index, int number, bool* destination);
  int GetBitBools(int column_index, int number, char* destination);

  // The Consume methods printf the output to screen. This is useful for
  // correctness checking, for running benchmarks you should likely redefine
  // them to do nothing.
  void ConsumeDoubles(int column_index, int number, const double* destination);
  void ConsumeInts(int column_index, int number, const int32* destination);
  void ConsumeByteBools(int column_index, int number, const bool* destination);
  void ConsumeBitBools(int column_index, int number, const char* destination);

private:
  vector<ColumnServer *> column_servers_;
};

class DoubleTestColumnServer : public ColumnServer {
public:
  DoubleTestColumnServer(int query_size)
  : ColumnServer(query_size), counter(0.0) {}

  int GetDoubles(int number, double *destination) {
    number = Serve(number);
    for (int i = 0; i < number; ++i) {
      destination[i] = Generate();
    }
    return number;
  }

private:
  double Generate() {
    return ((counter++) + 0.1) - 5.0;
  }
  double counter;
};

// Column Server implementations.
class IntTestColumnServer : public ColumnServer {
public:
  IntTestColumnServer(int query_size)
  : ColumnServer(query_size), counter(-5) {}

  int GetInts(int number, int *destination) {
    number = Serve(number);
    for (int i = 0; i < number; ++i) {
      destination[i] = Generate();
    }
    return number;
  }

private:
  int Generate() {
    return counter++;
  }
  int counter;
};

class BoolTestColumnServer : public ColumnServer {
public:
  BoolTestColumnServer(int query_size)
  : ColumnServer(query_size), counter(0) {}

  int GetByteBools(int number, bool *destination) {
    number = Serve(number);
    for (int i = 0; i < number; ++i) {
      destination[i] = Generate();
    }
    return number;
  }

  virtual int GetBitBools(int number, char* destination) {
    number = Serve(number);
    int n = (number + 7) * sizeof(*destination) / 8;
    memset(destination, 0, n);
    for (int i = 0; i < number; ++i) {
      destination[i / 8] |= (Generate() << (i & 7));
    }
    return number;
  }

private:
  char Generate() {
    return (counter++ % 5) > 2;
  }
  char counter;
};

// TestDataServer implementation.
TestDataServer::TestDataServer(const vector<int> &column_types) {
  int query_size = 10;
  vector<int>::const_iterator it;
  for (it = column_types.begin(); it != column_types.end(); ++it) {
    switch(*it) {
      case 1: column_servers_.push_back(new IntTestColumnServer(query_size));
        break;
      case 2: column_servers_.push_back(new DoubleTestColumnServer(query_size));
        break;
      case 3: column_servers_.push_back(new BoolTestColumnServer(query_size));
        break;
      default: assert(false);
    }
  }
}

TestDataServer::~TestDataServer() {
  vector<ColumnServer *>::iterator it;
  for (it = column_servers_.begin(); it != column_servers_.end(); ++it) {
    delete *it;
  }
}

int TestDataServer::GetDoubles(int c, int n, double* d) {
  double res = column_servers_[c]->GetDoubles(n, d);
  printf("Generating doubles:\n");
  ConsumeDoubles(c, res, d);
  printf("End generating:\n");
  return res;
}

int TestDataServer::GetInts(int c, int n, int32* d) {
  int res = column_servers_[c]->GetInts(n, d);
  printf("Generating int:\n");
  ConsumeInts(c, res, d);
  printf("End generating:\n");
  return res;
}

int TestDataServer::GetByteBools(int c, int n, bool* d) {
  printf("Generating byte bools:\n");
  int res = column_servers_[c]->GetByteBools(n, d);
  ConsumeByteBools(c, res, d);
  printf("End generating:\n");
  return res;
}

int TestDataServer::GetBitBools(int c, int n, char* d) {
  printf("Generating bit bools:\n");
  int res = column_servers_[c]->GetBitBools(n, d);
  ConsumeBitBools(c, res, d);
  printf("End generating:\n");
  return res;
}

void TestDataServer::ConsumeDoubles(int column_index, int number,
                                    const double* d) {
  printf("Consuming doubles:\n");
  for (int i = 0; i < number; ++i) {
    printf("C%d: %f\n", column_index, d[i]);
  }
  printf("End consuming\n");
}

void TestDataServer::ConsumeInts(int column_index, int number, const int32* d) {
  printf("Consuming ints:\n");
  for (int i = 0; i < number; ++i) {
    printf("C%d: %d\n", column_index, d[i]);
  }
  printf("End consuming\n");
}

void TestDataServer::ConsumeByteBools(int column_index, int number,
                                      const bool* d) {
  printf("Consuming byte bools:\n");
  for (int i = 0; i < number; ++i) {
    printf("C%d: %s\n", column_index, d[i] ? "TRUE" : "FALSE");
  }
}

void TestDataServer::ConsumeBitBools(int column_index, int number,
                                     const char* d) {
  printf("Consuming bit bools:\n");
  int pos = 0;
  char mask = 1;
  for (int i = 0; i < number; ++i) {
    if (!mask) {
      mask = 1;
      pos += 1;
    }
    printf("C%d: %s\n", column_index, (d[pos] & mask) ? "TRUE" : "FALSE");
    mask <<= 1;
  }
  printf("End consuming\n");
}

// PerfDataServer

class PerfDataServer : public Server {
 public:
  // The column_types vector should
  // contain types of columns - if column_types[i] = j, then the i-th
  // (0-indexed) column of the input is of type j (where 1 means int, 2 means
  // double and 3 means bool).
  PerfDataServer(const vector<int> &column_types, bool save_memory = false);
  ~PerfDataServer();
  int GetDoubles(int column_index, int number, double* destination);
  int GetInts(int column_index, int number, int32* destination);
  int GetByteBools(int column_index, int number, bool* destination);
  int GetBitBools(int column_index, int number, char* destination);

  // The Consume methods do nothing
  void ConsumeDoubles(int column_index, int number, const double* destination);
  void ConsumeInts(int column_index, int number, const int32* destination);
  void ConsumeByteBools(int column_index, int number, const bool* destination);
  void ConsumeBitBools(int column_index, int number, const char* destination);

 private:
  vector<ColumnServer *> column_servers_;
  struct timeval time_started_;
};

PerfDataServer::PerfDataServer(const vector<int> &column_types, bool save_memory) {
  int query_size = 20000000;
  if (save_memory) {
    query_size /= 2;
  }
  vector<int>::const_iterator it;
  for (it = column_types.begin(); it != column_types.end(); ++it) {
    switch(*it) {
      case 1: column_servers_.push_back(new IntColumnServer(query_size));
              break;
      case 2: column_servers_.push_back(new DoubleColumnServer(query_size));
              break;
      case 3: column_servers_.push_back(new BoolColumnServer(query_size));
              break;
      default: assert(false);
    }
  }

  // Timer
  gettimeofday(&time_started_, NULL);
}

PerfDataServer::~PerfDataServer() {
  vector<ColumnServer *>::iterator it;
  for (it = column_servers_.begin(); it != column_servers_.end(); ++it) {
    delete *it;
  }

  // Destroy timer
  struct timeval time_finished;
  gettimeofday(&time_finished, NULL);
  long sec = time_finished.tv_sec - time_started_.tv_sec;
  long msec = (time_finished.tv_usec - time_started_.tv_usec)/1000;
  if(msec < 0){
      msec += 1000;
      sec  -= 1;
  }
  printf("%ld.%04ld\n", sec, msec);
}

int PerfDataServer::GetDoubles(int c, int n, double* d) {
  double res = column_servers_[c]->GetDoubles(n, d);
  ConsumeDoubles(c, res, d);
  return res;
}

int PerfDataServer::GetInts(int c, int n, int32* d) {
  int res = column_servers_[c]->GetInts(n, d);
  ConsumeInts(c, res, d);
  return res;
}

int PerfDataServer::GetByteBools(int c, int n, bool* d) {
  return column_servers_[c]->GetByteBools(n, d);
}

int PerfDataServer::GetBitBools(int c, int n, char* d) {
  return column_servers_[c]->GetBitBools(n, d);
}

void PerfDataServer::ConsumeDoubles(int column_index, int number,
                                  const double* d) {
}

void PerfDataServer::ConsumeInts(int column_index, int number, const int32* d) {
}

void PerfDataServer::ConsumeByteBools(int column_index, int number,
                                    const bool* d) {
}

void PerfDataServer::ConsumeBitBools(int column_index, int number,
                                   const char* d) {
}


// Utilities for ColumnMap.
vector<int> CreateVector(int c1) {
  vector<int> result;
  result.push_back(c1);
  return result;
}

vector<int> CreateVector(int c1, int c2) {
  vector<int> result = CreateVector(c1);
  result.push_back(c2);
  return result;
}

vector<int> CreateVector(int c1, int c2, int c3) {
  vector<int> result = CreateVector(c1, c2);
  result.push_back(c3);
  return result;
}

vector<int> CreateVector(int c1, int c2, int c3, int c4) {
  vector<int> result = CreateVector(c1, c2, c3);
  result.push_back(c4);
  return result;
}

// Column maps for queries. Correspond to the maps in sample queries given in
// the Wiki. Define your own if you want to run your own test queries.
//
// Recall: 1: INT, 2: DOUBLE, 3: BOOL.
vector<int> ColumnMap(int query_id) {
  vector<int> result;
  switch(query_id) {
    case 1: return CreateVector(1, 1, 2, 3);
    case 2: return CreateVector(1, 1);
    case 3: return CreateVector(2);
    case 4: return CreateVector(1, 2);
    case 5: return CreateVector(1, 1);
    case 6: return CreateVector(2, 1, 1, 3);
    case 7: return CreateVector(2, 1);
    case 8: return CreateVector(3);
    case 9: return CreateVector(1, 2, 2);
    case 10: return CreateVector(3, 1);
    case 11: return CreateVector(1, 2);
    case 12: return CreateVector(1, 1, 2);
    default: assert(false);
  }
}

Server *CreateServer(int query_id, std::string variant) {
  if (variant == "default") {
    return new RealDataServer(ColumnMap(query_id));
  } else if (variant == "test") {
    return new TestDataServer(ColumnMap(query_id));
  } else if (variant == "perf") {
    bool save_memory = (query_id == 7);
    return new PerfDataServer(ColumnMap(query_id), save_memory);
  } else {
    assert(false);
  }
}
