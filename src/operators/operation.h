// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef OPERATION_H
#define OPERATION_H

#include <iostream>
#include <fstream>
#include <vector>
#include <tr1/unordered_map>
#include "node.h"
#include "column.h"
#include "factory.h"
#include "expression.h"
#include "keyvalue.h"

#include "proto/operations.pb.h"

using std::vector;

class Operation : public Node {
 protected:
  vector<Column*> cache;
 public:
  virtual vector<int> getTypes() = 0;
  virtual vector<Column*>* pull() = 0;

  int consume() {
    vector<Column*>* ptr = pull();

    if (ptr->empty()) {
      return 0;
    }

    for (unsigned i = 0 ; i < ptr->size() ; ++i) {
      (*ptr)[i]->consume(i, Factory::server);
    }

    return (*ptr)[0]->size;
  }

};

class ScanOperation : public Operation {
  vector<ColumnProvider*> providers;
 public:
  ScanOperation(const query::ScanOperation& oper) {
    int n = oper.column_size();
    cache = vector<Column*>(n);
    providers = vector<ColumnProvider*>(n);

    for (int i = 0 ; i < n ; ++i) {
      providers[i] = Factory::createColumnProvider(
          oper.column().Get(i), oper.type().Get(i));
    }
  }

  vector<Column*>* pull() {
    for (unsigned i = 0 ; i < providers.size() ; ++i) {
      cache[i] = providers[i]->pull();
    }
    return &cache;
  }

  vector<int> getTypes() {
    vector<int> result(providers.size());
    for (unsigned i = 0 ; i < providers.size() ; ++i) {
      result[i] = providers[i]->getType();
    }
    return result;
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << "ScanOperation {";
    for (unsigned i = 0 ; i < providers.size() ; ++i) {
      providers[i]->debugPrint(output);
      output << ", ";
    }
    return output << "}\n";
  }

  ~ScanOperation() {
    // TODO: free providers
  }
};

class ComputeOperation : public Operation {
  Operation* source;
  vector<Expression*> expressions;
 public:
  ComputeOperation(const query::ComputeOperation& oper) {
    source = Factory::createOperation(oper.source());

    int n = oper.expressions_size();
    expressions = vector<Expression*>(n);
    cache = vector<Column*>(n);

    vector<int> types = source->getTypes();
    for (int i = 0 ; i < n ; ++i) {
      expressions[i] = Factory::createExpression(
          oper.expressions().Get(i), types);
    }
  }

  vector<Column*>* pull() {
    vector<Column*>* sourceColumns = source->pull();
    for (unsigned i = 0 ; i < cache.size() ; ++i) {
      cache[i] = expressions[i]->pull(sourceColumns);
    }
    return &cache;
  }

  vector<int> getTypes() {
    vector<int> result(expressions.size());
    for (unsigned i = 0 ; i < expressions.size() ; ++i) {
      result[i] = expressions[i]->getType();
    }
    return result;
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << "ComputeOperation { \nsource = ";
    source->debugPrint(output);
    for (unsigned i = 0 ; i < expressions.size() ; ++i) {
      expressions[i]->debugPrint(output);
      output << ", ";
    }
    return output << "}\n";
  }
};

class FilterOperation : public Operation {
  Operation* source;
  Expression* condition;
  vector<Column*> result;
 public:
  FilterOperation(const query::FilterOperation& oper) {
    source = Factory::createOperation(oper.source());
    vector<int> types = source->getTypes();
    result = vector<Column*>(types.size());
    condition = Factory::createExpression(oper.expression(), types);

    for (unsigned i = 0 ; i < result.size() ; ++i) {
      result[i] = Factory::createColumnFromType(types[i]);
    }
  }

  vector<Column*>* pull() {
    vector<Column*>* sourceColumns = source->pull();
    Column* cond = condition->pull(sourceColumns);

    if (result[0]->size == 0) {
      return sourceColumns;
    }

    int ret = static_cast<ColumnChunk<char>*>(cond)->alwaysTrueOrFalse(); 
    if (ret == 1) {
      return sourceColumns;
    } else if (ret == -1) {
      return pull();
    } else {
      for (unsigned k = 0 ; k < result.size() ; ++k) {
        (*sourceColumns)[k]->filter(cond, result[k]);
      }
    }

    return &result;
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << "FilterOperation { source =";
    source->debugPrint(output);
    output << "expression = ";
    condition->debugPrint(output);
    return output << "}\n";
  }

  vector<int> getTypes() {
    return source->getTypes();
  }
};

typedef std::tr1::unordered_map<Key, Value, KeyHash, KeyEq> MapType;

class GroupByOperation : public Operation {
  Operation* source;
  vector<int> groupByColumn;
  vector<int> aggregations; // non negative sum on idx, -1 count
  MapType* m;
  MapType::iterator it;
 public:
  GroupByOperation(const query::GroupByOperation& oper) {
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

  vector<Column*>* pull();

  std::ostream& debugPrint(std::ostream& output) {
    output << "GroupByOperation { source = ";
    source->debugPrint(output);
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

  vector<int> getTypes() {
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
};

#endif // OPERATION_H

