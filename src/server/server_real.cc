// Copyright 2012 Google Inc. All Rights Reserved.
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <vector>
#include <utility>
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

// A class serving data from a single column.
class ColumnServer {
 public:
  ColumnServer(int query_size) : rows_left_(query_size) {}

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
    if (N >= rows_left_) {
      N = rows_left_;
    }
    rows_left_ -= N;
    return N;
  }

 private:
  int rows_left_;
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
  }

  int GetDoubles(int number, double *destination) {
    printf("SERVING %d to %d, zoom %d, normalcy %s\n",
           low_range_, high_range_, zoom_range_, normal_ ? "TRUE" : "FALSE");
    number = Serve(number);
    for (int i = 0; i < number; ++i) {
      destination[i] = Generate();
    }
    return number;
  }

 private:
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
  }

  int GetInts(int number, int *destination) {
    printf("SERVING %d to %d, normalcy %s\n",
           low_range_, high_range_, normal_ ? "TRUE" : "FALSE");
    number = Serve(number);
    for (int i = 0; i < number; ++i) {
      destination[i] = Generate();
    }
    return number;
  }

 private:
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
  }

  int GetByteBools(int number, bool *destination) {
    printf("SERVING probability %d\n", probability_);
    number = Serve(number);
    for (int i = 0; i < number; ++i) {
      destination[i] = Generate();
    }
    return number;
  }

  virtual int GetBitBools(int number, char* destination) {
    printf("SERVING probability %d\n", probability_);
    number = Serve(number);
    for (int i = 0; i < number; ++i) {
      destination[i / 8] |= (Generate() << (i & 7));
    }
    return number;
  }

 private:
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

Server *CreateServer(int query_id) {
  return new RealDataServer(ColumnMap(query_id));
}
