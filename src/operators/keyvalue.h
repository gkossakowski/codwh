// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef KEYVALUE_H
#define KEYVALUE_H

#include "column.h"

class Key {
 public:
  int n;
  any_t* keys;
  Key(const Key& clone) {
    n = clone.n;
    keys = new any_t[n];
    memcpy(keys, clone.keys, sizeof(*keys) * n);
  }

  Key(vector<Column*>* sources, vector<int>* check, int idx) {
    n = check->size(); //ugly
    keys = new any_t[n];
    memset(keys, 0, sizeof(*keys) * n);
    for (int i = 0 ; i < n ; ++i) {
      (*sources)[(*check)[i]]->fill(&keys[i], idx);
    }
  }

  void putInto(vector<Column*>* result, int idx) const {
    for (int i = 0 ; i < n ; ++i) {
      (*result)[i]->take(keys[i], idx);
    }
  }

  ~Key() {
    delete[] keys;
  }
};

class Value {
 public:
  int n;
  any_t* values;

  Value(const Value& clone) {
    n = clone.n;
    values = new any_t[n];
    memcpy(values, clone.values, n * sizeof(any_t));
  }

  Value(vector<Column*>* sources, vector<int>* aggregate, int idx) {
    n = aggregate->size();
    values = new any_t[n];
    memset(values, 0, n * sizeof(any_t));
    update(sources, aggregate, idx);
  }

  void putInto(vector<Column*>* result, int idx, int keyN) {
    for (int i = 0 ; i < n ; ++i) {
      (*result)[keyN + i]->take(values[i], idx);
    }
  }

  void update(vector<Column*>* sources, vector<int>* aggregate, int idx) {
    for (int i = 0 ; i < n ; ++i) {
      int v = (*aggregate)[i];
      if (v == -1) {
        values[i].int32 += 1;
      } else {
        (*sources)[v]->addTo(&values[i], idx);
      }
    }
  }

  ~Value() {
    delete[] values;
  }
};

typedef struct {
  long operator() (const Key& a) const {
    long h = 0;
    for (int i = 0 ; i < a.n ; ++i) {
      unsigned val = *((unsigned*) (a.keys + i)) ^ *(((unsigned*) (a.keys + i)) + 1);
      h = ( h << 4 ) ^ ( h >> 28 ) ^ val;
    }
    return h;
  }
} KeyHash;

typedef struct {
  long operator() (const Key& a, const Key& b) const {
    return a.n == b.n && 0 == memcmp(a.keys, b.keys, a.n * sizeof(any_t));
  }
} KeyEq;



#endif
