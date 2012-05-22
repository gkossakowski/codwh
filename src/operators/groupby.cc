// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "operation.h"

#include <set>
#include "keyvalue.h"

using std::set;

GroupByOperation::GroupByOperation(const query::GroupByOperation& oper) {
  m = NULL;
  source = Factory::createOperation(oper.source());

  groupByColumn = vector<int>(oper.group_by_column_size());
  for (unsigned i = 0 ; i < groupByColumn.size() ; ++i) {
    groupByColumn[i] = oper.group_by_column().Get(i);
  }

  aggregations = vector<int>(oper.aggregations_size());
  for (unsigned i = 0 ; i < aggregations.size() ; ++i) {
    query::Aggregation agr = oper.aggregations().Get(i);
    if (agr.type() == query::Aggregation::SUM) {
      aggregations[i] = agr.aggregated_column();
    } else if (agr.type() == query::Aggregation::COUNT) {
      aggregations[i] = -1;
    } else {
      assert(false);
    }
  }

  vector<int> types = getTypes();
  cache = vector<Column*>(types.size());
  for (unsigned int i = 0 ; i < cache.size() ; ++i) {
    cache[i] = Factory::createColumnFromType(types[i]);
  }
}

std::ostream& GroupByOperation::debugPrint(std::ostream& output) {
  output << "GroupByOperation { source = " << *source;
  output << "columns = ";
  for (unsigned i = 0 ; i < groupByColumn.size() ; ++i) {
    output << groupByColumn[i] << ",";
  }
  output << "\naggregations = ";
  for (unsigned i = 0 ; i < aggregations.size() ; ++i) {
    output << aggregations[i] << ",";
  }
  return output << "}\n";
}

vector<int> GroupByOperation::getTypes() {
  vector<int> sourceTypes = source->getTypes();
  vector<int> result;
  for (unsigned i = 0 ; i < groupByColumn.size() ; ++i) {
    result.push_back(sourceTypes[groupByColumn[i]]);
  }
  for (unsigned i = 0 ; i < aggregations.size() ; ++i) {
    if (aggregations[i] == -1) {
      result.push_back(query::ScanOperation_Type_INT);
    } else {
      int type = sourceTypes[aggregations[i]];
      if (type == query::ScanOperation_Type_BOOL) {
        result.push_back(query::ScanOperation_Type_INT);
      } else {
        result.push_back(type);
      }
    }
  }
  return result;
}

vector<int> GroupByOperation::getUsedColumnsId() {
  vector<int> sourceTypes = source->getTypes();
  set<int> result;
  for (unsigned i = 0 ; i < groupByColumn.size() ; ++i) {
    result.insert(groupByColumn[i]);
  }
  for (unsigned i = 0 ; i < aggregations.size() ; ++i) {
    if (aggregations[i] != -1) {
      result.insert(aggregations[i]);
    }
  }
  return vector<int>(result.begin(), result.end());
}

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

GroupByOperation::~GroupByOperation() {
  delete source;
  for (unsigned k = 0 ; k < cache.size() ; ++k) {
    delete cache[k];
  }

  if (m != NULL) {
    delete m;
  }
}
