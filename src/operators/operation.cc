// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "operation.h"

int Operation::consume() {
  vector<Column*>* ptr = pull();

  if (ptr->empty()) {
    return 0;
  }

  for (unsigned i = 0 ; i < ptr->size() ; ++i) {
    (*ptr)[i]->consume(i, Factory::server);
  }

  return (*ptr)[0]->size;
}

// ScanOperation {{{ 
ScanOperation::ScanOperation(const query::ScanOperation& oper) {
  int n = oper.column_size();
  cache = vector<Column*>(n);
  providers = vector<ColumnProvider*>(n);

  for (int i = 0 ; i < n ; ++i) {
    providers[i] = Factory::createColumnProvider(
        oper.column().Get(i), oper.type().Get(i));
  }
}

vector<Column*>*
ScanOperation::pull() {
  for (unsigned i = 0 ; i < providers.size() ; ++i) {
    cache[i] = providers[i]->pull();
  }
  return &cache;
}

vector<int>
ScanOperation::getTypes() {
  vector<int> result(providers.size());
  for (unsigned i = 0 ; i < providers.size() ; ++i) {
    result[i] = providers[i]->getType();
  }
  return result;
}

std::ostream&
ScanOperation::debugPrint(std::ostream& output) {
  output << "ScanOperation {";
  for (unsigned i = 0 ; i < providers.size() ; ++i) {
    output << *providers[i] << ",\n";
  }
  return output << "}\n";
}

ScanOperation::~ScanOperation() {
  for (unsigned i = 0 ; i < providers.size() ; ++i) {
    delete providers[i];
  }
}

// }}}

// ComputeOperation {{{
ComputeOperation::ComputeOperation(const query::ComputeOperation& oper) {
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

vector<Column*>* ComputeOperation::pull() {
  vector<Column*>* sourceColumns = source->pull();
  for (unsigned i = 0 ; i < cache.size() ; ++i) {
    cache[i] = expressions[i]->pull(sourceColumns);
  }
  return &cache;
}

vector<int> ComputeOperation::getTypes() {
  vector<int> result(expressions.size());
  for (unsigned i = 0 ; i < expressions.size() ; ++i) {
    result[i] = expressions[i]->getType();
  }
  return result;
}

std::ostream& ComputeOperation::debugPrint(std::ostream& output) {
  output << "ComputeOperation { \nsource = " << *source;
  output << "expressions = ";
  for (unsigned i = 0 ; i < expressions.size() ; ++i) {
    output << *expressions[i] << ", ";
  }
  return output << "}\n";
}

ComputeOperation::~ComputeOperation() {
  for (unsigned i = 0 ; i < expressions.size() ; ++i) {
    delete expressions[i];
  }
  delete source;
}
// }}}

// FilterOperation {{{
FilterOperation::FilterOperation(const query::FilterOperation& oper) {
  source = Factory::createOperation(oper.source());
  vector<int> types = source->getTypes();
  result = vector<Column*>(types.size());
  condition = Factory::createExpression(oper.expression(), types);

  for (unsigned i = 0 ; i < result.size() ; ++i) {
    result[i] = Factory::createColumnFromType(types[i]);
  }
}

vector<Column*>* FilterOperation::pull() {
  vector<Column*>* sourceColumns = source->pull();
  Column* cond = condition->pull(sourceColumns);

  if (sourceColumns->empty()) {
    return sourceColumns;
  }

  for (unsigned k = 0 ; k < result.size() ; ++k) {
    (*sourceColumns)[k]->filter(cond, result[k]);
  }

  if (result[0]->size == 0 && (*sourceColumns)[0]->size > 0) {
    // No results, reapeat
    return pull();
  }

  return &result;
}

std::ostream& FilterOperation::debugPrint(std::ostream& output) {
  output << "FilterOperation { source =" << *source;
  return output << "condition = " << *condition << "}\n";
}

vector<int> FilterOperation::getTypes() {
  return source->getTypes();
}

FilterOperation::~FilterOperation() {
  for (unsigned i = 0 ; i < result.size() ; ++i) {
    delete result[i];

  }
  delete source;
  delete condition;
}

// }}}
