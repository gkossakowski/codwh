// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "factory.h"

#include <algorithm>

#include "operation.h"
#include "column.h"

using std::max;

Server* Factory::server;

Operation*
Factory::createOperation(const query::Operation& operation) {
  if (operation.has_scan()) {
    return new ScanOperation(operation.scan());
  } else if (operation.has_compute()) {
    return new ComputeOperation(operation.compute());
  } else {
    assert(false);
    return NULL;
  }
}

ColumnProvider*
Factory::createColumnProvider(int columnId, int type) {
  switch (type) {
    case query::ScanOperation_Type_INT:
      return new ColumnProviderImpl<int>(columnId);
    case query::ScanOperation_Type_DOUBLE:
      return new ColumnProviderImpl<double>(columnId);
    case query::ScanOperation_Type_BOOL:
      return new ColumnProviderImpl<char>(columnId);
    default:
      assert(false);
  }
}

static int findType(
    const query::Expression& expression, const vector<int>& providers) {
  switch (expression.operator_()) {
    case query::Expression::COLUMN:
      return providers[expression.column_id()];
    case query::Expression::ADD:
    case query::Expression::SUBTRACT:
    case query::Expression::MULTIPLY:
      assert(expression.children_size() == 2);
      return max(findType(expression.children().Get(0), providers),
          findType(expression.children().Get(1), providers));
    default:
      assert(false);
  }
  return 0;
}

template<class T>
static Expression* createExpressionImpl(
    const query::Expression& expression, vector<int>& providers) {
  switch (expression.operator_()) {
    case query::Expression::COLUMN:
      return new ExpressionColumn<T>(expression.column_id());
    case query::Expression::ADD:
      return new ExpressionAdd<T>(
          Factory::createExpression(expression.children().Get(0), providers),
          Factory::createExpression(expression.children().Get(1), providers)
          );
    default:
      assert(false);
  }
  return NULL;
}

Expression*
Factory::createExpression(
    const query::Expression& expression, vector<int>& providers) {
  switch (findType(expression, providers)) {
    case 1:
      return createExpressionImpl<int>(expression, providers);
    case 2:
      return createExpressionImpl<double>(expression, providers);
    case 3:
      return createExpressionImpl<char>(expression, providers);
    default:
      assert(false);
      return NULL;
  }
}
