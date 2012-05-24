// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "operation.h"

#include "distributed/node.h"

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
        oper.column().Get(i), (query::ColumnType) oper.type().Get(i));
  }
}

vector<Column*>*
ScanOperation::pull() {
  for (unsigned i = 0 ; i < providers.size() ; ++i) {
    cache[i] = providers[i]->pull();
  }
  return &cache;
}

vector<query::ColumnType>
ScanOperation::getTypes() {
  vector<query::ColumnType> result(providers.size());
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

  vector<query::ColumnType> types = source->getTypes();
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

vector<query::ColumnType> ComputeOperation::getTypes() {
  vector<query::ColumnType> result(expressions.size());
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
  vector<query::ColumnType> types = source->getTypes();
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

vector<query::ColumnType> FilterOperation::getTypes() {
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

// ShuffleOperation {{{
ShuffleOperation::ShuffleOperation(const query::ShuffleOperation& oper) {
  source = Factory::createOperation(oper.source());
  receiversCount = oper.receiverscount();
}

vector<Column*>* ShuffleOperation::pull() {
  assert(false); // should use ShuffleOperation::bucketsPull()
}

vector< vector<Column*> >* ShuffleOperation::bucketsPull() {
  source->pull();
  // TODO: implement!
  assert(false);
  return NULL;
}

std::ostream& ShuffleOperation::debugPrint(std::ostream& output) {
  output << "shuffleOperation { " << *source;
  return output << "receiversCount = " << receiversCount << "}\n";
}

vector<query::ColumnType> ShuffleOperation::getTypes() {
  // TODO: implement
  return source->getTypes();
}

ShuffleOperation::~ShuffleOperation() {
  delete source;
}
// }}}

// UnionOperation {{{
UnionOperation::UnionOperation(const query::UnionOperation& oper) {
  assert(oper.column_size() == oper.type_size());

  firstPull = true;

  sourcesNode = vector<int>(oper.source_size());
  sourcesStripe = vector<int>(oper.source_size());
  for (unsigned i = 0 ; i < sourcesNode.size() ; ++i) {
    sourcesNode[i] = oper.source().Get(i).node();
    sourcesStripe[i] = oper.source().Get(i).stripe();
    nodeToStripe[sourcesNode[i]] = sourcesStripe[i];
  }

  columns = vector<int>(oper.column_size());
  int maxx = -1;
  for (unsigned i = 0 ; i < columns.size() ; ++i) {
    columns[i] = oper.column().Get(i);
    if (columns[i] > maxx)
      maxx = columns[i];
  }

  int typeSize = maxx + 1;

  types = vector<query::ColumnType>(typeSize);
  unsigned n = oper.type_size();
  for (unsigned i = 0 ; i < n ; ++i) {
    types[columns[i]] = (query::ColumnType) oper.type().Get(i);
  }
}

vector<Column*>* UnionOperation::pull() {
  if (firstPull) {
    // Ask everyone
    for (unsigned i = 0 ; i < sourcesNode.size() ; ++i) {
      query::DataRequest request;
      request.set_node(sourcesNode[i]);
      request.set_stripe(sourcesStripe[i]);
      request.set_number(1); // TODO: set it more reasonable
      global::worker->send(&request);
    }
    firstPull = false;
  }

  // preparing new data
  query::Communication* message;
  bool hasRow = false;
  while (!hasRow && (message = global::worker->getMessage(true)) != NULL) {
    if (message->has_data_response()) {
      // ask for more
      query::DataResponse dataResponse = message->data_response();
      if (dataResponse.number() > 0) {
        query::DataRequest request;
        request.set_node(dataResponse.node());
        request.set_stripe(nodeToStripe[dataResponse.node()]);
        request.set_number(1); // TODO: set it more reasonable
        global::worker->send(&request);
      }

      assert(dataResponse.data().data_size() == dataResponse.data().type_size());
      int n = dataResponse.data().data_size();
      for (int i = 0 ; i < n ; ++i) {
        // TODO parse it
        Packet packet(dataResponse.data().data().Get(i).c_str(),
            1 /* TODO: I don't know what should be passed */ );
        // TODO: save it in cache
      }

      // TODO: check (potentially set hasRow = true)

    } else {
      // store future requests
      global::worker->parseMessage(message, false);
    }
  }

  
  // TODO: implement
  vector<Column*>* tmp = new vector<Column*>;
  tmp->push_back(new ColumnChunk<double>());
  return tmp;
}

std::ostream& UnionOperation::debugPrint(std::ostream& output) {
  output << "UnionOperation { ";
  return output << "}\n";
}

vector<query::ColumnType> UnionOperation::getTypes() {
  return types;
}
// }}}

// FinalOperation {{{
FinalOperation::FinalOperation(const query::FinalOperation& oper) {
  source = Factory::createOperation(oper.source());
}

vector<Column*>* FinalOperation::pull() {
  return source->pull();
}

std::ostream& FinalOperation::debugPrint(std::ostream& output) {
  output << "FinalOperation { " << *source;
  return output << "}\n";
}

vector<query::ColumnType> FinalOperation::getTypes() {
  return source->getTypes();
}

FinalOperation::~FinalOperation() {
  delete source;
}
// }}}
