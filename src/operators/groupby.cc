// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "operation.h"

#include "keyvalue.h"

vector<Column*>*
GroupByOperation::pull() {
  vector<Column*>* sourceColumns;

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

    it = m->begin();
  }

  // serve it
  int i = 0;
  while (it != m->end() && i < DEFAULT_CHUNK_SIZE) {
    it->first.putInto(&cache, i);
    it->second.putInto(&cache, i, (int)groupByColumn.size());
    ++it;
    ++i;
  }

  if (it == m->end()) {
    m->clear();
    it = m->end();
  }

  for (unsigned k = 0 ; k < cache.size() ; ++k) {
    cache[k]->size = i;
  }

  return &cache;
}
