// Copyright 2012 Google Inc. All Rights Reserved.
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include "server.h"

using ::std::vector;

class SimpleServer : public Server {
 public:
  SimpleServer(int query_id);
  int GetDoubles(int column_index, int number, double* destination);
  int GetInts(int column_index, int number, int32* destination);
  int GetByteBools(int column_index, int number, bool* destination);
  int  GetBitBools(int column_index, int number, char* destination);

  void ConsumeDoubles(int column_index, int number, const double* destination);
  void ConsumeInts(int column_index, int number, const int32* destination);
  void ConsumeByteBools(int column_index, int number, const bool* destination);
  void ConsumeBitBools(int column_index, int number, const char* destination);

 private:
  int QuerySize();
  int ColumnNumber();
  int ColumnType(int column_index);

  int query_id_;
  vector<int> served_entries_;
};

int SimpleServer::QuerySize() {
  return 10;
}

int SimpleServer::ColumnNumber() {
  switch(query_id_) {
    case 0: return 4;
    default: return 0;
  }
}

// 0 - non-existent
// 1 - int
// 2 - double
// 3 - bool
int SimpleServer::ColumnType(int column_index) {
  switch(query_id_) {
    case 0:
      switch(column_index) {
        case 0: return 1;
        case 1: return 1;
        case 2: return 2;
        case 3: return 3;
        default: return 0;
      }
    default:
      return 0;
  }
}

SimpleServer::SimpleServer(int query_id)
  : query_id_(query_id),
    served_entries_(ColumnNumber()) {
}

int SimpleServer::GetDoubles(int c, int n, double* d) {
  assert(ColumnType(c) == 2);
  for (int i = 0; i < n; ++i) {
    if (served_entries_[c] == QuerySize()) return i;
    d[i] = (double) rand() / (double) rand();
    served_entries_[c]++;
  }
  return n;
}

int SimpleServer::GetInts(int c, int n, int32* d) {
  assert(ColumnType(c) == 1);
  for (int i = 0; i < n; ++i) {
    if (served_entries_[c] == QuerySize()) return i;
    d[i] = (int32) rand();
    served_entries_[c]++;
  }
  return n;
}

int SimpleServer::GetByteBools(int c, int n, bool* d) {
  assert(ColumnType(c) == 3);
  for (int i = 0; i < n; ++i) {
    if (served_entries_[c] == QuerySize()) return i;
    d[i] = rand() % 2 == 0;
    served_entries_[c]++;
  }
  return n;
}

int SimpleServer::GetBitBools(int c, int n, char* d) {
  assert(ColumnType(c) == 3);
  int initial_entries = served_entries_[c];
  for (int i = 0; i < (n+7) / 8; ++i) {
    if (served_entries_[c] == QuerySize()) break;
    d[i] = (char) rand();
    served_entries_[c] = std::min(QuerySize(), served_entries_[c] + 8);
  }
  return served_entries_[c] - initial_entries;
}

void SimpleServer::ConsumeDoubles(int column_index, int number,
                                  const double* d) {
  for (int i = 0; i < number; ++i) {
    printf("C%d: %f\n", column_index, d[i]);
  }
}

void SimpleServer::ConsumeInts(int column_index, int number, const int32* d) {
  for (int i = 0; i < number; ++i) {
    printf("C%d: %d\n", column_index, d[i]);
  }
}

void SimpleServer::ConsumeByteBools(int column_index, int number,
                                    const bool* d) {
  for (int i = 0; i < number; ++i) {
    printf("C%d: %s\n", column_index, d[i] ? "TRUE" : "FALSE");
  }
}

void SimpleServer::ConsumeBitBools(int column_index, int number,
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

Server *CreateServer(int query_id) {
  return new SimpleServer(query_id);
}
