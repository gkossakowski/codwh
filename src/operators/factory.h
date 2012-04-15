// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef FACTORY_H
#define FACTORY_H

#include <vector>
#include <cmath>

#include "proto/operations.pb.h"
#include "server.h"

using std::vector;

class Operation;
class ColumnProvider;
class Expression;

class Factory {
 public:
  static Server* server;
  static Operation* createOperation(const query::Operation& operation);
  static ColumnProvider* createColumnProvider(int columnId, int type);
  static Expression* createExpression(
      const query::Expression& expression, vector<int>& providers);
};

#endif

