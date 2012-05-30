// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef FACTORY_H
#define FACTORY_H

#include <vector>
#include <cmath>

#include "proto/operations.pb.h"
#include "server.h"
#include "node_environment/node_environment.h"

using std::vector;

class Operation;
class ColumnProvider;
class Expression;
class Column;

class Factory {
 public:
  static Server* server;
  static Operation* createOperation(const query::Operation& operation);
  static ColumnProvider* createColumnProvider(int columnId, query::ColumnType type);
  static ColumnProvider* createFileColumnProvider(DataSourceInterface** source,
      int columnId, query::ColumnType type);
  static Column* createColumnFromType(query::ColumnType type);
  static Expression* createExpression(
      const query::Expression& expression, vector<query::ColumnType>& providers);
};

#endif

