// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "operation.h"

#include "distributed/node.h"
#include "node_environment/sink_server_proxy.h"

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

// ScanOperationOwn {{{ 
ScanOperationOwn::ScanOperationOwn(const query::ScanOperationOwn& oper) {
  int n = oper.column_size();
  cache = vector<Column*>(n);
  source = global::worker->openSourceInterface(oper.source());
  providers = vector<ColumnProvider*>(n);

  for (int i = 0 ; i < n ; ++i) {
    providers[i] = Factory::createFileColumnProvider(source,
        oper.column().Get(i), (query::ColumnType) oper.type().Get(i));
  }
}

vector<Column*>*
ScanOperationOwn::pull() {
  for (unsigned i = 0 ; i < providers.size() ; ++i) {
    cache[i] = providers[i]->pull();
  }
  return &cache;
}

vector<query::ColumnType>
ScanOperationOwn::getTypes() {
  vector<query::ColumnType> result(providers.size());
  for (unsigned i = 0 ; i < providers.size() ; ++i) {
    result[i] = providers[i]->getType();
  }
  return result;
}

std::ostream&
ScanOperationOwn::debugPrint(std::ostream& output) {
  output << "ScanOperationOwn {";
  for (unsigned i = 0 ; i < providers.size() ; ++i) {
    output << *providers[i] << ",\n";
  }
  return output << "}\n";
}

ScanOperationOwn::~ScanOperationOwn() {
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
  assert(oper.column_size() == oper.type_size());
  for (int i = 0; i < oper.column_size(); i++) {
    columns.push_back(oper.column(i));
    columnTypes.push_back(oper.type(i));
  }
  for (int i = 0; i < oper.hash_column_size(); i++) {
    hashColumns.push_back(oper.hash_column(i));
  }
  buckets = vector< vector<Column*> >(receiversCount);
  for (unsigned int i = 0; i < buckets.size(); i++) {
    buckets[i] = std::vector<Column*>(columnTypes.size());
    for (unsigned j = 0; j < columnTypes.size(); j++) {
      buckets[i][j] = Factory::createColumnFromType(columnTypes[j]);
    }
  }
  columnsHash = Factory::createColumnFromType(query::HASH);
}

vector<Column*>* ShuffleOperation::pull() {
  assert(false); // should use ShuffleOperation::bucketsPull()
}

void hashSourceColumns(const vector<Column*>* sourceColumns, vector<int> which,
                 Column* columnsHash) {
  assert(columnsHash->getType() == query::HASH);
  assert(which.size() > 0);
  int size = (*sourceColumns)[which[0]]->size;
  columnsHash->size = size;
  columnsHash->zero();
  for (unsigned int i = 0; i < which.size(); i++) {
    (*sourceColumns)[which[i]]->hash(columnsHash);
  }
}

vector< vector<Column*> >* ShuffleOperation::bucketsPull() {
  vector<Column*>* sourceColumns = source->pull();
  // zero column sizes in all buckets
  for (unsigned int i = 0; i < receiversCount; i++) {
    for (unsigned int j = 0; j < buckets[i].size(); j++) {
      buckets[i][j]->size = 0;
    }
  }
  if (hashColumns.size() > 0) {
    assert(receiversCount > 1);
    hashSourceColumns(sourceColumns, hashColumns, columnsHash);
    size_t* hashes = static_cast<ColumnChunk<size_t>*>(columnsHash)->chunk;
    for (int i = 0; i < columnsHash->size; i++) {
      int bucketNumber = hashes[i] % receiversCount;
      vector<Column*>* bucket = &buckets[bucketNumber];
      any_t data;
      for (unsigned j = 0; j < bucket->size() ; j++) {
        (*sourceColumns)[j]->fill(&data, i);
        Column* col = (*bucket)[j];
        col->take(data, col->size);
        col->size++;
      }
    }
  } else {
    assert(receiversCount == 1);
    for (unsigned i = 0; i < columns.size() ; i++) {
      Column* sourceCol = (*sourceColumns)[columns[i]];
      Column* destCol = buckets[0][i];
      any_t data;
      for (int j = 0; j < sourceCol->size; j++) {
        sourceCol->fill(&data, j);
        destCol->take(data, destCol->size);
        destCol->size++;
      }
    }
  }
  return &buckets;
}

std::ostream& ShuffleOperation::debugPrint(std::ostream& output) {
  output << "shuffleOperation { " << *source;
  return output << "receiversCount = " << receiversCount << "}\n";
}

vector<query::ColumnType> ShuffleOperation::getTypes() {
  return columnTypes;
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
    columns[i] = oper.column(i);
    if (columns[i] > maxx)
      maxx = columns[i];
  }

  int typeSize = maxx + 1;
  columnIsUsed = vector<bool>(typeSize, false);
  for (unsigned i = 0 ; i < columns.size() ; ++i) {
    columnIsUsed[columns[i]] = true;
  }

  types = vector<query::ColumnType>(typeSize);
  unsigned n = oper.type_size();
  for (unsigned i = 0 ; i < n ; ++i) {
    types[columns[i]] = (query::ColumnType) oper.type().Get(i);
  }

  Column *col = new ColumnChunk<int>(); // TODO: implement dummy to save time on allocation
  col->size = 0;
  eof.push_back(col);
}

vector<Column*>* UnionOperation::pull() {
  if (firstPull) {
    // Ask everyone
    for (unsigned i = 0 ; i < sourcesNode.size() ; ++i) {
      global::worker->sendRequest(sourcesStripe[i], 1,
		      sourcesNode[i]); // TODO: set it more reasonable
    }
    firstPull = false;
  }

  // preparing new data
  query::DataResponse* dataResponse;
  while (cache.size() == 0) {
    global::worker->getResponse();
    dataResponse = global::worker->responses.front();
    global::worker->responses.pop();

    // ask for more
    if (dataResponse->number() > 0) {
      query::DataRequest request;
      global::worker->sendRequest(nodeToStripe[dataResponse->node()], 1, // TODO: nie wiem czy to dobre odwzorowanie
		      	          dataResponse->node()); // TODO: set it more reasonable

    } else {
      finished++; // got EOF
    }

    assert(dataResponse->data().data_size() == dataResponse->data().type_size());
    consume(dataResponse);

    if (finished == sourcesNode.size())
      return &eof;
  }

  // ask cache
  vector<Column*> *data = cache.front();
  cache.pop();
  return data;
  // TODO: chunk deallocation
}

void UnionOperation::consume(query::DataResponse *response) {
  printf("UnionOperation::consume...\n");
  int from_row = 0;
  assert(response->data().data(0).size() % global::getTypeSize(response->data().type(0)) == 0);
  int size = response->data().data(0).size() /
             global::getTypeSize(response->data().type(0));
  int chunk_size;
  printf("size = %d\n", size);

  query::DataPacket *packet = response->mutable_data();
  vector<Column*> *chunk;
  Column *col;

  while (from_row < size) {
    chunk = new vector<Column*>;

    chunk_size = std::min(DEFAULT_CHUNK_SIZE, size - from_row);

    for (uint32_t i = 0; i < types.size(); i++) {
      if (!columnIsUsed[i]) {
        // TODO: dont waste memory here
        // TODO introduce ColumnChunk<void> that always fails
        col = new ColumnChunk<int>();
        col->size = chunk_size;
        printf("adding dummy column with index %d\n", i);
        chunk->push_back(col);
        continue;
      }
      if (types[i] == query::INT)
        col = deserializeChunk<int>(from_row, packet->data(i).c_str(), chunk_size);
      else if (types[i] == query::DOUBLE)
        col = deserializeChunk<double>(from_row, packet->data(i).c_str(), chunk_size);
      else if (types[i] == query::BOOL)
        col = deserializeChunk<char>(from_row, packet->data(i).c_str(), chunk_size);
      else assert(false);
      chunk->push_back(col);
    }
    cache.push(chunk);
    from_row += chunk_size;
  }
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
  sinkProxy = new SinkToServerProxy(global::worker->openSinkInterface());
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

int FinalOperation::consume() {
  vector<Column*>* ptr = pull();

  if (ptr->empty()) {
    return 0;
  }

  for (unsigned i = 0 ; i < ptr->size() ; ++i) {
    (*ptr)[i]->consume(i, sinkProxy);
  }

  return (*ptr)[0]->size;
}

FinalOperation::~FinalOperation() {
  delete sinkProxy;
  delete source;
}
// }}}
