// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef KEYVALUE_H
#define KEYVALUE_H

#include "column.h"

class Key {
 public:
  int n;
  vector<Column*>* columnChunks;
  vector<int>* check;
  int idx;
  Key(const Key& clone) {
    n = clone.n;
    columnChunks = clone.columnChunks;
    check = clone.check;
    idx = clone.idx;
    //memcpy(keys, clone.keys, sizeof(*keys) * n);
  }

  Key(vector<Column*>* columnChunks, vector<int>* check, int idx) {
    n = check->size(); //ugly
    this->columnChunks = columnChunks;
    this->check = check;
    this->idx = idx;
  }
  
  inline const any_t getMember(const int i) const {
    int colNumber = (*check)[i];
    any_t any;
    memset(&any, 0, sizeof(any_t));
    (*columnChunks)[colNumber]->fill(&any, idx);
    return any;
  }

  void putInto(vector<Column*>* result, int target_idx) const {
    for (int i = 0 ; i < n ; ++i) {
      any_t any = getMember(i);
      (*result)[i]->take(any, target_idx);
    }
  }
};

class Value {
 public:
  int n;
  int idx;
  // target columns where we store (aggregated) values
  vector<Column*>* values;
//  any_t* values;

  Value(const Value& clone) {
    n = clone.n;
    idx = clone.idx;
    values = clone.values;
  }

  Value(vector<Column*>* const sources, vector<int>* const aggregate, const int source_idx, 
        vector<Column*>* const target, const int target_idx) {
    n = aggregate->size();
    values = target;
    idx = target_idx;
    for (int i=0; i<n; i++) {
      // little hack...
      (*target)[i]->size = idx+1;
      (*target)[i]->zero(idx);
    }
    update(sources, aggregate, source_idx);
  }

  void putInto(vector<Column*>* result, int idx, int keyN) {
    for (int i = 0 ; i < n ; ++i) {
      any_t any;
      // TODO: remove?
      memset(&any, 0, sizeof(any_t));
      (*values)[i]->fill(&any, this->idx);
      (*result)[keyN + i]->take(any, idx);
    }
  }

  void update(vector<Column*>* sources, vector<int>* aggregate, int source_idx) {
//    printf("updating value with source_idx = %d and idx = %d\n", source_idx, idx);
    for (int i = 0 ; i < n ; ++i) {
      int v = (*aggregate)[i];
      Column* col = (*values)[i];
      if (v == -1) {
        static_cast<ColumnChunk<int>*>(col)->chunk[idx] += 1;
//        values[i].int32 += 1;
      } else {
//        printf("sources[%d]..\n", v);
//        (*sources)[v]->debugPrint();
//        printf("col..\n");
//        col->debugPrint();
        col->add((*sources)[v], source_idx, idx);
//        (*sources)[v]->addTo(&values[i], idx);
      }
    }
  }

  ~Value() {
//    delete[] values;
  }
};

typedef struct {
  long operator() (const Key& a) const {
    long h = 0;
    for (int i = 0 ; i < a.n ; ++i) {
      any_t any = a.getMember(i);
      unsigned val = *((unsigned*) (&any)) ^ *(((unsigned*) (&any)) + 1);
      h = ( h << 4 ) ^ ( h >> 28 ) ^ val;
    }
    return h;
  }
} KeyHash;

typedef struct {
  long operator() (const Key& a, const Key& b) const {
    if (a.n != b.n)
      return false;
    for (int i =0; i < a.n; i++) {
      any_t any1 = a.getMember(i);
      any_t any2 = b.getMember(i);
      if (memcmp(&any1, &any2, sizeof(any_t) != 0))
        return false;
    }
    return true;
  }
} KeyEq;



#endif
