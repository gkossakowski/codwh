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
  } else if (operation.has_scan_file()) {
    return new ScanFileOperation(operation.scan_file());
  } else if (operation.has_compute()) {
    return new ComputeOperation(operation.compute());
  } else if (operation.has_filter()) {
    return new FilterOperation(operation.filter());
  } else if (operation.has_group_by()) {
    return new GroupByOperation(operation.group_by());
  } else if (operation.has_shuffle()) {
    return new ShuffleOperation(operation.shuffle());
  } else if (operation.has_union_()) {
    return new UnionOperation(operation.union_());
  } else if (operation.has_final()) {
    return new FinalOperation(operation.final());
  } else {
    std::cout << "ERROR: unknown operation" <<
        operation.DebugString() << "\n";
    assert(false);
    return NULL;
  }
}

ColumnProvider*
Factory::createColumnProvider(int columnId, query::ColumnType type) {
  switch (type) {
    case query::INT:
      return new ColumnProviderServer<int>(columnId);
    case query::DOUBLE:
      return new ColumnProviderServer<double>(columnId);
    case query::BOOL:
      return new ColumnProviderServer<char>(columnId);
    default:
      assert(false);
  }
}

ColumnProvider*
Factory::createFileColumnProvider(DataSourceInterface* source,
    int columnId, query::ColumnType type) {
  switch (type) {
    case query::INT:
      return new ColumnProviderFile<int>(source, columnId);
    case query::DOUBLE:
      return new ColumnProviderFile<double>(source, columnId);
    case query::BOOL:
      return new ColumnProviderFile<char>(source, columnId);
    default:
      assert(false);
  }
}

Column*
Factory::createColumnFromType(query::ColumnType type) {
  switch (type) {
    case query::INT:
      return new ColumnChunk<int>();
    case query::DOUBLE:
      return new ColumnChunk<double>();
    case query::BOOL:
      return new ColumnChunk<char>();
    case query::HASH:
      return new ColumnChunk<size_t>();
    default:
      assert(false);
  }
}

// findType {{{
static int findType(
    const query::Expression& expression, const vector<query::ColumnType>& providers) {
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
    const query::Expression& expression, vector<query::ColumnType>& providers) {
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
    const query::Expression& expression, vector<query::ColumnType>& providers) {
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
    const query::Expression& expression, vector<query::ColumnType>& providers) {
  if (expression.operator_() == query::Expression::GREATER ||
      expression.operator_() == query::Expression::LOWER ||
      expression.operator_() == query::Expression::EQUAL ||
      expression.operator_() == query::Expression::NOT_EQUAL) {
    int leftType = findType(expression.children().Get(0), providers);
    int rightType = findType(expression.children().Get(1), providers);
    switch (100 * leftType + rightType) {
      case (query::INT * 100 + query::INT):
        return createExpressionLogic<int, int>(expression, providers);
      case (query::DOUBLE * 100 + query::DOUBLE): 
        return createExpressionLogic<double, double>(expression, providers);
      case (query::INT * 100 + query::DOUBLE):
        return createExpressionLogic<int, double>(expression, providers);
      case (query::DOUBLE * 100 + query::INT):
        return createExpressionLogic<double, int>(expression, providers);
      case (query::BOOL * 100 + query::BOOL):
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
