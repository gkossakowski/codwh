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

/** Base from all operations: scan, filter, group by, compute */
class Operation : public Node {
 protected:
  vector<Column*> cache;
 public:
  /** Get return types */
  virtual vector<int> getTypes() = 0;
  /** Pull next chunk of data */
  virtual vector<Column*>* pull() = 0;
  /** consume output at server */
  int consume();
};

class ScanOperation : public Operation {
  vector<ColumnProvider*> providers;
 public:
  ScanOperation(const query::ScanOperation& oper);
  vector<Column*>* pull();
  vector<int> getTypes();
  std::ostream& debugPrint(std::ostream& output);
  ~ScanOperation();
};

class ComputeOperation : public Operation {
  Operation* source;
  vector<Expression*> expressions;
 public:
  ComputeOperation(const query::ComputeOperation& oper);
  vector<Column*>* pull();
  vector<int> getTypes();
  std::ostream& debugPrint(std::ostream& output);
  ~ComputeOperation();
};

class FilterOperation : public Operation {
  Operation* source;
  Expression* condition;
  vector<Column*> result;
 public:
  FilterOperation(const query::FilterOperation& oper);
  vector<Column*>* pull();
  std::ostream& debugPrint(std::ostream& output);
  vector<int> getTypes();
  ~FilterOperation();
};

typedef std::tr1::unordered_map<Key, Value, KeyHash, KeyEq> MapType;

class GroupByOperation : public Operation {
  Operation* source;
  vector<int> groupByColumn;
  vector<int> aggregations; // non negative sum on idx, -1 count
  MapType* m;
  MapType::iterator it;
 public:
  GroupByOperation(const query::GroupByOperation& oper);
  vector<Column*>* pull();
  std::ostream& debugPrint(std::ostream& output);
  vector<int> getTypes();
  vector<int> getUsedColumnsId();
  ~GroupByOperation();
};

class ShuffleOperation : public Operation {
  Operation* source;
  unsigned int receiversCount;
 public:
  ShuffleOperation(const query::ShuffleOperation& oper);
  vector<Column*>* pull();
  std::ostream& debugPrint(std::ostream& output);
  vector<int> getTypes();
  ~ShuffleOperation();
};

class UnionOperation : public Operation {
  vector<int> sources;
  vector<int> types;
 public:
  UnionOperation(const query::UnionOperation& oper);
  vector<Column*>* pull();
  std::ostream& debugPrint(std::ostream& output);
  vector<int> getTypes();
};

#endif // OPERATION_H

