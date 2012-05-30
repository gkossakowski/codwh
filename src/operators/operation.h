// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef OPERATION_H
#define OPERATION_H

#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
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
  /** Get return types, ints are used as constants corresponding to types from ScanOperation */
  virtual vector<query::ColumnType> getTypes() = 0;
  /** Pull next chunk of data */
  virtual vector<Column*>* pull() = 0;
  virtual vector< vector<Column*> >* bucketsPull() {
    assert(false);
  }
  /** consume output at server */
  virtual int consume();
  virtual ~Operation() {};
};

class ScanOperation : public Operation {
  vector<ColumnProvider*> providers;
 public:
  ScanOperation(const query::ScanOperation& oper);
  vector<Column*>* pull();
  vector<query::ColumnType> getTypes();
  std::ostream& debugPrint(std::ostream& output);
  ~ScanOperation();
};

class ScanFileOperation : public Operation {
  vector<ColumnProvider*> providers;
  DataSourceInterface* source;
 public:
  ScanFileOperation(const query::ScanFileOperation& oper);
  vector<Column*>* pull();
  vector<query::ColumnType> getTypes();
  std::ostream& debugPrint(std::ostream& output);
  ~ScanFileOperation();
};

class ComputeOperation : public Operation {
  Operation* source;
  vector<Expression*> expressions;
 public:
  ComputeOperation(const query::ComputeOperation& oper);
  vector<Column*>* pull();
  vector<query::ColumnType> getTypes();
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
  vector<query::ColumnType> getTypes();
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
  vector<query::ColumnType> getTypes();
  vector<int> getKeyColumnsId();
  vector<int> getUsedColumnsId();
  ~GroupByOperation();
};

class ShuffleOperation : public Operation {
  Operation* source;
  unsigned int receiversCount;
  std::vector<int> columns;
  std::vector<query::ColumnType> columnTypes;
  std::vector<int> hashColumns;
  vector< vector<Column*> > buckets;
  Column* columnsHash;
 public:
  ShuffleOperation(const query::ShuffleOperation& oper);
  vector<Column*>* pull(); // can't use!
  vector< vector<Column*> >* bucketsPull();
  std::ostream& debugPrint(std::ostream& output);
  vector<query::ColumnType> getTypes();
  ~ShuffleOperation();
};

class UnionOperation : public Operation {
  vector<int> sourcesNode;
  vector<int> sourcesStripe;
  vector<int> columns;
  vector<bool> columnIsUsed;
  uint32_t finished;
  vector<query::ColumnType> types;
  std::queue<vector<Column*>*> cache;
  vector<Column*>* tmp;
  bool firstPull;
  void processReceivedData(query::DataResponse *response);
  vector<Column*> eof;
  void deleteChunkData(vector<Column*>* chunk);
 public:
  UnionOperation(const query::UnionOperation& oper);
  vector<Column*>* pull();
  std::ostream& debugPrint(std::ostream& output);
  vector<query::ColumnType> getTypes();
  ~UnionOperation();
};

class FinalOperation : public Operation {
  Operation* source;
  Server* sinkProxy;
 public:
  FinalOperation(const query::FinalOperation& oper);
  vector<Column*>* pull();
  std::ostream& debugPrint(std::ostream& output);
  vector<query::ColumnType> getTypes();
  int consume();
  ~FinalOperation();
};

#endif // OPERATION_H

