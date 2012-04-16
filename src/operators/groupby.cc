// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "operation.h"

#include "keyvalue.h"
#include <tr1/unordered_map>

int Key::n;
int Value::n;

typedef std::tr1::unordered_map<Key, Value, KeyHash, KeyEq> MapType;

vector<Column*>*
GroupByOperation::pull() {
  vector<Column*>* sourceColumns;

  static MapType *m;

  if (m == NULL) {
    m = new MapType();
    // collect the data
    do {
      sourceColumns = source->pull();
      int n = (*sourceColumns)[0]->size;
      for (int i = 0 ; i < n ; ++i) {
        Key key(sourceColumns, &groupByColumn, i);  
        typeof(m->end()) it = m->find(key);
        if (it == m->end()) {
          // on insert
          m->insert(MapType::value_type(key, Value(sourceColumns, &aggregations, i)));
        } else {
          // on update
          it->second.update(sourceColumns, &aggregations, i);;
        }
      }
    } while ((*sourceColumns)[0]->size > 0);
  }

  // serve it
  static MapType::iterator it = m->begin();
  int i = 0;
  while (it != m->end() && i < DEFAULT_CHUNK_SIZE) {
    it->first.putInto(&cache, i);
    it->second.putInto(&cache, i);
    ++it;
    ++i;
  }

  for (unsigned k = 0 ; k < cache.size() ; ++k) {
    cache[k]->size = i;
  }

  return &cache;
}
