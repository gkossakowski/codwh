// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef KEYVALUE_H
#define KEYVALUE_H

#include "column.h"

class Key {
 public:
  Key(vector<Column*>* sources, vector<int>* check, int idx) {
    int n = check->size();
    keys = new any_t[n];
    memset(keys, 0, sizeof(*keys) * n);
    for (int i = 0 ; i < n ; ++i) {
      (*sources)[(*check)[i]]->fill(&keys[i], idx);
    }
  }

  any_t* keys;

  ~Key() {
    delete[] keys;
  }
};

#endif
