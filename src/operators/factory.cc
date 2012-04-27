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
  } else if (operation.has_filter()) {
    return new FilterOperation(operation.filter());
  } else if (operation.has_group_by()) {
    return new GroupByOperation(operation.group_by());
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

Column*
Factory::createColumnFromType(int type) {
  switch (type) {
    case query::ScanOperation_Type_INT:
      return new ColumnChunk<int>();
    case query::ScanOperation_Type_DOUBLE:
      return new ColumnChunk<double>();
    case query::ScanOperation_Type_BOOL:
      return new ColumnChunk<char>();
    default:
      assert(false);
  }
}

// findType {{{
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
    case query::Expression::FLOATING_DIVIDE:
      assert(expression.children_size() == 2);
      return query::ScanOperation::DOUBLE;
    case query::Expression::LOG: // find argument type
      return findType(expression.children().Get(0), providers);
    case query::Expression::NEGATE:
      assert(expression.children_size() == 1);
      return findType(expression.children().Get(0), providers);
    case query::Expression::LOWER:
    case query::Expression::GREATER:
    case query::Expression::EQUAL:
    case query::Expression::NOT_EQUAL:
      // find argument type
      return findType(expression.children().Get(0), providers);
    case query::Expression::NOT:
    case query::Expression::OR:
    case query::Expression::AND:
      return query::ScanOperation::BOOL;
    case query::Expression::IF:
      return findType(expression.children().Get(1), providers);
    case query::Expression::CONSTANT:
      if (expression.has_constant_int32()) {
        return query::ScanOperation::INT;
      } else if (expression.has_constant_double()) {
        return query::ScanOperation::DOUBLE;
      } else if (expression.has_constant_bool()) {
        return query::ScanOperation::BOOL;
      } else {
        assert(false);
      }
    default:
      std::cout << expression.DebugString() << "\n";
      assert(false);
  }
  return 0;
}
// }}}

// createExpressionImpl {{{
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
    case query::Expression::SUBTRACT:
      return new ExpressionMinus<T>(
          Factory::createExpression(expression.children().Get(0), providers),
          Factory::createExpression(expression.children().Get(1), providers)
          );
    case query::Expression::MULTIPLY:
      return new ExpressionMultiply<T>(
          Factory::createExpression(expression.children().Get(0), providers),
          Factory::createExpression(expression.children().Get(1), providers)
          );
    case query::Expression::FLOATING_DIVIDE:
      return new ExpressionDivide<T>(
          Factory::createExpression(expression.children().Get(0), providers),
          Factory::createExpression(expression.children().Get(1), providers)
          );
    case query::Expression::AND:
      return new ExpressionAnd(
          Factory::createExpression(expression.children().Get(0), providers),
          Factory::createExpression(expression.children().Get(1), providers)
          );
    case query::Expression::OR:
      return new ExpressionOr(
          Factory::createExpression(expression.children().Get(0), providers),
          Factory::createExpression(expression.children().Get(1), providers)
          );
    case query::Expression::NOT:
      return new ExpressionNot(
          Factory::createExpression(expression.children().Get(0), providers)
          );
    case query::Expression::NEGATE:
      return new ExpressionNegate<T>(
          Factory::createExpression(expression.children().Get(0), providers)
          );
    case query::Expression::LOWER:
    case query::Expression::GREATER:
    case query::Expression::EQUAL:
    case query::Expression::NOT_EQUAL:
      assert(false); // should be handled elsewhere
      return NULL;
    case query::Expression::IF:
      return new ExpressionIf<T>(
          Factory::createExpression(expression.children().Get(0), providers),
          Factory::createExpression(expression.children().Get(1), providers),
          Factory::createExpression(expression.children().Get(2), providers)
          );
    case query::Expression::LOG:
      return new ExpressionLog<T>(
          Factory::createExpression(expression.children().Get(0), providers)
          );
    case query::Expression::CONSTANT:
      if (expression.has_constant_int32()) {
        return new ExpressionConstant<T>((T) expression.constant_int32());
      } else if (expression.has_constant_double()) {
        return new ExpressionConstant<T>((T) expression.constant_double());
      } else if (expression.has_constant_bool()) {
        return new ExpressionConstant<char>((bool) expression.constant_bool());
      } else {
        assert(false);
      }
    default:
      assert(false);
  }
  return NULL;
}
// }}}

template<class TL, class TR>
static Expression* createExpressionLogic(
    const query::Expression& expression, vector<int>& providers) {
  Expression* left =
    Factory::createExpression(expression.children().Get(0), providers);
  Expression* right =
    Factory::createExpression(expression.children().Get(1), providers);

  if (expression.operator_() == query::Expression::GREATER) {
    return new ExpressionLower<TR, TL>(right, left);
  } else if (expression.operator_() == query::Expression::LOWER) {
    return new ExpressionLower<TL, TR>(left, right);
  } else if (expression.operator_() == query::Expression::EQUAL) {
    return new ExpressionEqual<TL, TR>(left, right);
  } else if (expression.operator_() == query::Expression::NOT_EQUAL) {
    return new ExpressionNotEqual<TL, TR>(left, right);
  } else {
    assert(false);
  }
}


Expression*
Factory::createExpression(
    const query::Expression& expression, vector<int>& providers) {
  if (expression.operator_() == query::Expression::GREATER ||
      expression.operator_() == query::Expression::LOWER ||
      expression.operator_() == query::Expression::EQUAL ||
      expression.operator_() == query::Expression::NOT_EQUAL) {
    int leftType = findType(expression.children().Get(0), providers);
    int rightType = findType(expression.children().Get(1), providers);
    switch (10 * leftType + rightType) {
      case 11:
        return createExpressionLogic<int, int>(expression, providers);
      case 22: 
        return createExpressionLogic<double, double>(expression, providers);
      case 12:
        return createExpressionLogic<int, double>(expression, providers);
      case 21:
        return createExpressionLogic<double, int>(expression, providers);
      case 33:
        return createExpressionLogic<char, char>(expression, providers);
      default:
        assert(false); // unknown combination
        return NULL;
    }
      
  }
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
