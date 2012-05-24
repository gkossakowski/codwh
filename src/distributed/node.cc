#include <vector>
#include <queue>

#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"
#include "node.h"
#include "operators/factory.h"

namespace global {
  WorkerNode* worker;
}

WorkerNode::WorkerNode(NodeEnvironmentInterface *nei) : nei(nei) {
  global::worker = this;
}

query::Communication* WorkerNode::getMessage(bool blocking) {
  char *data;
  size_t data_len;
  query::Communication *message;

  if (!blocking) {
    data = nei->ReadPacketNotBlocking(&data_len);
    if (NULL == data)
      return NULL;
  }
  data = nei->ReadPacketBlocking(&data_len);
  message = new query::Communication();
  message->ParseFromString(string(data, data_len));

  delete[] data;
  return message;
}

void WorkerNode::parseMessage(query::Communication *message, bool allow_data) {
  if (message->stripe_size() > 0) {
    query::Communication::Stripe *st;
    for (int i = 0; i < message->stripe_size(); i++) {
      st = new query::Communication::Stripe();
      st->GetReflection()->Swap(message->mutable_stripe(i), st);
      jobs.push(st);
    }
  } else if (message->has_data_request()) {
    requests.push(message->release_data_request());
  } else if (allow_data && message->has_data_response()){
    // TODO : consume data
  } else assert(false);

  delete message;
}

void WorkerNode::getJob() {
  /** Wait until a job occures, than store it in a jobs queue. */
  std::cout << "Awaiting for a job" << std::endl;
  query::Communication *message;

  while (jobs.size() == 0) {
    message = getMessage(true);
    parseMessage(message, false); // there should be no incoming data while
                                  // awaiting for a job
  }
  return ;
}

void WorkerNode::getRequest() {
  /** Wait until a data request occures, than store it in a requests queue */
  std::cout << "Awaiting for data request" << std::endl;
  query::Communication *message;

  while (requests.size() == 0) {
    message = getMessage(true);
    parseMessage(message, true);
  }
  return ;
}

void WorkerNode::resetOutput(int buckets, vector<bool> &columns) {
  for (uint32_t i = 0; i < output.size(); i++)
    if (output[i].size() != 0) assert(false); // calling during processing is forbidden

  sent_columns.swap(columns);
  output.resize(0); // drop current objects (just in case)
  output.resize(buckets);
  pending_requests.resize(0);
  pending_requests.resize(buckets, 0);
  output_counters.resize(0);
  output_counters.resize(buckets, 0);
  consumers_map.resize(0);
  consumers_map.resize(buckets, -1);
  full_packets = 0;
}

void WorkerNode::parseRequests() {
  int bucket;
  query::DataRequest *request;

  while (requests.size() > 0) {
    request = requests.back();
    requests.pop();
    bucket = request->stripe() % output.size();
    consumers_map[bucket] = request->node();
    pending_requests[bucket] += request->number();

    flushBucket(bucket); // try to send data
    delete request;
  }
}

void WorkerNode::packData(vector<Column*> &data, int bucket) {
  queue<Packet*> &buck = output[bucket];

  if (buck.size() == 0 || buck.back()->full)
    buck.push(new Packet(data, sent_columns));
  buck.back()->consume(data);

  if (buck.back()->full)
    full_packets++;
  assert(full_packets <= MAX_OUTPUT_PACKETS);

  // read all incoming requests
  query::Communication *message;
  while ((message = getMessage(false)) != NULL) {
    parseMessage(message, false); // we should have all data already
  }

  bool flag;
  do {
    parseRequests();
    flag = (full_packets == MAX_OUTPUT_PACKETS);
    if (flag)
      getRequest();
  } while (flag);

  // finally, try to flush current bucket
  flushBucket(bucket);
}

void WorkerNode::flushBucket(int bucket) {
  if (pending_requests[bucket] == 0 || output[bucket].front()->full)
    return;
  assert(consumers_map[bucket] != -1); // we should know node number from request

  query::Communication com;
  query::DataResponse *response = com.mutable_data_response();
  query::DataPacket *packet;
  const google::protobuf::Reflection *r = response->data().GetReflection();
  string msg;

  response->set_node(nei->my_node_number());
  // send data while we have a full packet and a pending request
  while (pending_requests[bucket] > 0 && !output[bucket].front()->full) {
    packet = output[bucket].front()->serialize();
    output_counters[bucket]++; // increase packet number counter

    response->set_number(output_counters[bucket]); // set number
    r->Swap(response->mutable_data(), packet); // set data
    com.SerializeToString(&msg);
    nei->SendPacket(consumers_map[bucket], msg.c_str(), msg.size()); // send

    output[bucket].pop(); // remove packet from queue
    pending_requests[bucket]--;
    full_packets--;
    delete packet; // dump packet
  }
}

void WorkerNode::send(query::DataRequest* request) {
  string msg;
  request->SerializeToString(&msg);
  nei->SendPacket(request->node(), msg.c_str(), msg.size());
}

void WorkerNode::sendEof() {
  query::Communication com;
  query::DataResponse *response = com.mutable_data_response();
  string msg;

  response->set_node(nei->my_node_number());
  response->set_number(-1); // EOF response
  com.SerializeToString(&msg); // universal message for all consumers

  for (uint32_t i = 0; i < output.size(); i++) {
    while (consumers_map[i] == -1) { // unknown consumer!
      getRequest();
      parseRequests(); // updates the consumer_map while parsing a request
    }
    nei->SendPacket(consumers_map[i], msg.c_str(), msg.size());
  }
}

int WorkerNode::execPlan(query::Operation *op) {
  printf("Worker[%d] stripe[%d] job proto tree:\n%s\n",
         nei->my_node_number(), stripe, op->DebugString().c_str());
  Operation* operation = Factory::createOperation(*op);
  FinalOperation* finalOperation = dynamic_cast<FinalOperation*>(operation);

  if (NULL != finalOperation) {
    while (finalOperation->consume() > 0) {
    }
  } else {
    vector< vector<Column*> > *buckets;
    int buckets_num = 0;
    int size;
    do {
      size = 0;
      // pull buckets
      buckets = operation->bucketsPull();
      if (buckets_num == 0) {
        // first iteration; we have to reset output buffer
        buckets_num = buckets->size();
        vector<bool> columns; // TODO: mask unused columns
        columns.resize(buckets_num, true);
        resetOutput(buckets_num, columns);
      }


      for (int i = 0; i < buckets_num; i++) {
        size += (*buckets)[i][0]->size;
        packData((*buckets)[i], i);
      }
    } while (size > 0);

    // we have all data; try to send it
    assert(buckets_num > 0);
    for (int i = 0; i < buckets_num; i++) {
      if (!output[i].back()->full) { // close packets
        output[i].back()->full = true;
        full_packets++;
      }
      flushBucket(i);
    }

    // await for requests as long as everything is sent
    while (full_packets > 0) {
      getRequest();
      parseRequests();
    }
    sendEof(); // confirm end of data stream
  }

  delete operation;
  return 0;
}

void WorkerNode::run() {
  int ret = 0;
  while (0 == ret) {
    if (0 == jobs.size()) getJob();
    query::Communication::Stripe *st;
    st = jobs.front();
    stripe = st->stripe();
    execPlan(st->mutable_operation());
    jobs.pop();
  };
  return ;
}
/*
 * Cuts query into stripes.
 *
 * Each stripe is created whenever there's group by operation
 * encountered in query tree.
 *
 * Each stripe (apart from the first and the last ones) has union
 * operation as a source and shuffle operation as a sink.
 * Two consecutive stripes should have column fields matching in
 * shuffle and union operations (so they can be plugged together).
 *
 * When Shuffle and Union operations are introduced their fields
 * respectively `receiversCount` and `source` are left uninitialized.
 * It's job of the other part of the scheduler to initialize them.
 */
vector<query::Operation> stripeOperation(const query::Operation query) {
  vector<query::Operation> stripes;
  query::Operation op = query;
  if (query.has_scan()) {
    query::Operation scanOwnOp;
    query::ScanOperationOwn& scanOwn = *scanOwnOp.mutable_scan_own();
    for (int i = 0; i < query.scan().column_size(); i++) {
      scanOwn.add_column(query.scan().column(i));
      switch (query.scan().type(i)) {
        case query::ScanOperation_Type_BOOL:
          scanOwn.add_type(query::BOOL);
          break;
        case query::ScanOperation_Type_INT:
          scanOwn.add_type(query::INT);
          break;
        case query::ScanOperation_Type_DOUBLE:
          scanOwn.add_type(query::DOUBLE);
          break;
        default:
          assert(false);
      }
    }
    stripes.push_back(scanOwnOp);
  } else if (query.has_compute()) {
    stripes = stripeOperation(query.compute().source());
    query::Operation lastStripe = stripes.back();
    stripes.pop_back();
    op.mutable_compute()->clear_source();
    op.mutable_compute()->mutable_source()->MergeFrom(lastStripe);
    stripes.push_back(op);
  } else if (query.has_filter()) {
    stripes = stripeOperation(query.filter().source());
    query::Operation lastStripe = stripes.back();
    stripes.pop_back();
    op.mutable_filter()->clear_source();
    op.mutable_filter()->mutable_source()->MergeFrom(lastStripe);
    stripes.push_back(op);
  } else if (query.has_group_by()) {
    stripes = stripeOperation(query.group_by().source());
    query::Operation lastStripe = stripes.back();
    stripes.pop_back();

    query::GroupByOperation groupBy = query.group_by();

    // columns needed by group by operations both for keys and values
    vector<int> columns;
    // columns used for hashing, subset of `columns`
    vector<int> keyColumns;
    // types of all columns of the source of group by
    vector<query::ColumnType> types;
    {
      GroupByOperation* groupByOp = static_cast<GroupByOperation*>(Factory::createOperation(op));
      columns = groupByOp->getUsedColumnsId();
      keyColumns = groupByOp->getKeyColumnsId();
      types = Factory::createOperation(groupBy.source())->getTypes();
    }

    query::Operation shuffle;
    shuffle.mutable_shuffle()->mutable_source()->MergeFrom(lastStripe);
    for (unsigned int i=0; i < columns.size(); i++) {
      shuffle.mutable_shuffle()->add_column(columns[i]);
      shuffle.mutable_shuffle()->add_type(types[columns[i]]);
    }
    for (unsigned int i = 0 ; i < keyColumns.size(); i++) {
      shuffle.mutable_shuffle()->add_hash_column(keyColumns[i]);
    }
    stripes.push_back(shuffle);

    groupBy.clear_source();
    query::UnionOperation* union_ = groupBy.mutable_source()->mutable_union_();
    for (unsigned i = 0; i < columns.size(); i++) {
      union_->add_column(columns[i]);
      union_->add_type(types[columns[i]]);
    }

    query::Operation groupByOp;
    groupByOp.mutable_group_by()->MergeFrom(groupBy);
    stripes.push_back(groupByOp);
  } else if (query.has_scan_own() || query.has_union_() || query.has_shuffle() || query.has_final()) {
    // this should never happen as those nodes are introduced by stripeOperation
    assert(false);
  }
  return stripes;
}

vector<query::ColumnType> getColumnTypes(const query::Operation& opProto) {
  Operation* op = Factory::createOperation(opProto);
  vector<query::ColumnType> result = op->getTypes();
  return result;
}

void addColumnsAndTypesToShuffle(query::ShuffleOperation& shuffle) {
  vector<query::ColumnType> types = getColumnTypes(shuffle.source());
  for (unsigned int i = 0; i < types.size(); i++) {
    shuffle.add_column(i);
    shuffle.add_type(types[i]);
  }
}

vector<query::Operation>* SchedulerNode::makeStripes(query::Operation query) {
  vector<query::Operation> stripes = stripeOperation(query);
  // add shuffle to the last but one stripe
  {
    query::Operation shuffleOp;
    query::Operation lastButOne = stripes.back();
    stripes.pop_back();
    shuffleOp.mutable_shuffle()->mutable_source()->MergeFrom(lastButOne);
    addColumnsAndTypesToShuffle(*shuffleOp.mutable_shuffle());
    stripes.push_back(shuffleOp);
  }
  // add the last stripe that is just union and final operation
  {
    query::Operation finalOp;
    query::UnionOperation& unionOp = *finalOp.mutable_final()->mutable_source()->mutable_union_();
    query::Operation& lastButOne = stripes.back();
    assert(lastButOne.has_shuffle());
    for (int i = 0; i < lastButOne.shuffle().column_size(); i++) {
      unionOp.add_column(lastButOne.shuffle().column(i));
      unionOp.add_type(lastButOne.shuffle().type(i));
    }
    stripes.push_back(finalOp);
  }
  vector<query::Operation> *stripes_ptr = new vector<query::Operation>(stripes.begin(), stripes.end());
  return stripes_ptr;
}

/*
 * Assigns nodes to union operation that is stored in a given stripe. This
 * method should not be called with first stripe that has no union operations.
 *
 * Nodes are specified as a range [nodeStart, nodeEnd).
 *
 * This method mutates passed stripe.
 */
void assignNodesToUnion(query::Operation& stripe, const vector< std::pair<int, int> > & stripeIds) {
  if (stripe.has_scan()) {
    // stripes should never have plain scan
    assert(false);
  } else if (stripe.has_compute()) {
    assignNodesToUnion(*stripe.mutable_compute()->mutable_source(), stripeIds);
  } else if (stripe.has_filter()) {
    assignNodesToUnion(*stripe.mutable_filter()->mutable_source(), stripeIds);
  } else if (stripe.has_group_by()) {
    assignNodesToUnion(*stripe.mutable_group_by()->mutable_source(), stripeIds);
  } else if (stripe.has_scan_own()) {
    // only the first stripe can have scan own operation and this method
    // should not be called for the first stripe
    assert(false);
  } else if (stripe.has_shuffle()) {
    assignNodesToUnion(*stripe.mutable_shuffle()->mutable_source(), stripeIds);
  } else if (stripe.has_union_()) {
    for (unsigned int i = 0; i < stripeIds.size(); i++) {
      query::UnionOperation_Source* src = stripe.mutable_union_()->add_source();
      src->set_node(stripeIds[i].first);
      src->set_stripe(stripeIds[i].second);
    }
  } else if (stripe.has_final()) {
    assignNodesToUnion(*stripe.mutable_final()->mutable_source(), stripeIds);
  }
}

void assignReceiversCount(query::Operation& stripe, int receiversCount) {
  // the last operation (the root of stripe) should be shuffle
  assert(stripe.has_shuffle());
  stripe.mutable_shuffle()->set_receiverscount(receiversCount);
}

int extractInputFilesNumber(const query::Operation& query) {
  if (query.has_scan()) {
    return query.scan().number_of_files();
  } else if (query.has_compute()) {
    return extractInputFilesNumber(query.compute().source());
  } else if (query.has_filter()) {
    return extractInputFilesNumber(query.filter().source());
  } else if (query.has_group_by()) {
    return extractInputFilesNumber(query.group_by().source());
  } else if (query.has_scan_own()) {
    // if we have scan_own it means that the original scan operation has been
    // erased and there's no way to extract number of input files
    assert(false);
  } else if (query.has_shuffle()) {
    return extractInputFilesNumber(query.shuffle().source());
  } else if (query.has_union_()) {
    // if query has union as its source then it means we cannot extract number
    // of files (probably the wrong stripe has been passed)
    assert(false);
  } else if (query.has_final()) {
    return extractInputFilesNumber(query.final().source());
  } else {
    // unknown case
    assert(false);
  }
}

/*
 * Assigns file to ScanOperationOwn. This method should be called with
 * the first stripe only.
 *
 * This method mutates passed stripe.
 */
void assignFileToScan(query::Operation& stripe, int fileToScan) {
  if (stripe.has_scan()) {
    // stripes should never have plain scan
    assert(false);
  } else if (stripe.has_compute()) {
    assignFileToScan(*stripe.mutable_compute()->mutable_source(), fileToScan);
  } else if (stripe.has_filter()) {
    assignFileToScan(*stripe.mutable_filter()->mutable_source(), fileToScan);
  } else if (stripe.has_group_by()) {
    assignFileToScan(*stripe.mutable_group_by()->mutable_source(), fileToScan);
  } else if (stripe.has_scan_own()) {
    stripe.mutable_scan_own()->set_source(fileToScan);
  } else if (stripe.has_shuffle()) {
    assignFileToScan(*stripe.mutable_shuffle()->mutable_source(), fileToScan);
  } else if (stripe.has_union_()) {
    // union is the source, probably the wrong stripe passed
    assert(false);
  } else if (stripe.has_final()) {
    assignFileToScan(*stripe.mutable_final()->mutable_source(), fileToScan);
  }
}

void SchedulerNode::schedule(vector<query::Operation> *stripes, uint32_t nodes, int numberOfFiles) {
  int nodesPerStripe = nodes / 2;
  // we don't handle situation when we have one worker only
  assert(nodesPerStripe > 0);
  // we divide workers into two groups that will be used for two consecutive layers of stripes
  vector<int> nodeIds;
  vector<int> nodeIdsNext;
  for (int i = 1; i <= nodesPerStripe; i++) {
    nodeIds.push_back(i);
  }
  for (uint32_t i = nodesPerStripe+1; i <= nodes; i++) {
    nodeIdsNext.push_back(i);
  }

  printf("nodeIds: ");
  for (unsigned int i = 0; i < nodeIds.size(); i++) {
    printf("%d, ", nodeIds[i]);
  }
  printf("\n");
  printf("nodeIdsNext: ");
  for (unsigned int i = 0; i < nodeIdsNext.size(); i++) {
    printf("%d, ", nodeIdsNext[i]);
  }
  printf("\n");

  vector< std::pair<int, int> > previousStripeIds;
  int stripeId = 0;
  // process the first stripe
  {
    query::Operation& firstStripe = stripes->front();
    assignReceiversCount(firstStripe, nodeIdsNext.size());
    for (int i = 0; i < numberOfFiles; i++) {
      query::Operation stripeForFile = firstStripe; // make copy
      assignFileToScan(stripeForFile, i);
      int nodeId = nodeIds[i%nodeIds.size()];
      sendJob(stripeForFile, nodeId, stripeId);
      previousStripeIds.push_back(std::make_pair(nodeId, stripeId));
      stripeId++;
    }
  }
  std::swap(nodeIds, nodeIdsNext);
  // process inner stripes
  for (unsigned i = 1; i + 1 < stripes->size(); i++) {
    query::Operation& stripe = (*stripes)[i];
    vector< std::pair<int, int> > stripeIds;
    // assign to union nodes and stripes that were
    // sent in the previous iteration
    assignNodesToUnion(stripe, previousStripeIds);
    assignReceiversCount(stripe, nodeIdsNext.size());
    for (unsigned int j = 0; j < nodeIds.size(); j++) {
      int nodeId = nodeIds[j%nodeIds.size()];
      // make local copy of stripe because sendJob destroys its input data
      query::Operation localStripe = stripe;
      sendJob(localStripe, nodeId, stripeId);
      stripeIds.push_back(std::make_pair(nodeId, stripeId));
      stripeId++;
    }
    previousStripeIds = stripeIds;
    std::swap(nodeIds, nodeIdsNext);
  }
  // schedule the last stripe, use all the remaining workers
  {
    query::Operation lastStripe = stripes->back();
    assignNodesToUnion(lastStripe, previousStripeIds);
    sendJob(lastStripe, 0, stripeId);
  }
}

void SchedulerNode::sendJob(query::Operation &op, uint32_t node, int stripeId) {
  printf("Enqueing stripe[%d] to worker[%d]\n\n", stripeId, node);
  nodesJobs[node].push_back(std::make_pair(stripeId, op));
}

void SchedulerNode::flushJobs() {
  const google::protobuf::Reflection *r;
  string msg;

  for (uint32_t node = 0; node < nodesJobs.size(); node++) {
    if (nodesJobs[node].size() == 0)
      continue;

    query::Communication com;
    for (uint32_t i = 0; i < nodesJobs[node].size(); i++) {
      com.add_stripe();
      com.mutable_stripe(i)->set_stripe(nodesJobs[node][i].first);
      r = com.stripe(i).operation().GetReflection();
      r->Swap(&nodesJobs[node][i].second, com.mutable_stripe(i)->mutable_operation());
    }

    com.SerializeToString(&msg);
    printf("Sending jobs to worker[%d]\n\n%s", node, com.DebugString().c_str());
    nei->SendPacket(node, msg.c_str(), msg.size());
  }
}

void SchedulerNode::run(const query::Operation &op) {
  std::cout << "Scheduling proto: " << op.DebugString() << "\n";
  int numberOfInputFiles = extractInputFilesNumber(op);
  printf("number of input files: %d\n", numberOfInputFiles);
  vector<query::Operation> *stripes = makeStripes(op);
  std::cout << "after striping: " << std::endl;
  for (unsigned i=0; i < stripes->size() ; i++) {
    std::cout << "STRIPE " << i << std::endl;
    std::cout << (*stripes)[i].DebugString() << std::endl;
  }
  schedule(stripes, nei->nodes_count() - 1, numberOfInputFiles);
  flushJobs();
  delete stripes;

  // TODO : switch to a worker mode
  // execPlan(finalOperation);
  return ;
}


Packet::Packet(vector<Column*> &view, vector<bool> &sent_columns)
  : size(0), full(false)
{
  size_t row_size = 0;
  columns.resize(view.size(), NULL);
  offsets.resize(view.size(), 0);
  types.resize(view.size(), query::INVALID_TYPE);
  sentColumns = sent_columns;

  // collect row size and types
  for (uint32_t i = 0; i < view.size(); i++) {
    types[i] = view[i]->getType();
    if (sentColumns[i]){
      row_size += global::getTypeSize(types[i]);
    } // otherwise we omit it
  }

  // compute maximum capacity
  capacity = MAX_PACKET_SIZE / row_size;

  // allocate space and reset data counters;
  for (uint32_t i = 0; i < view.size(); i++)
    if (sent_columns[i])
      columns[i] = new char[capacity * global::getTypeSize(types[i])];
}

/** Tries to consume a view. If it's too much data - returns false. */
void Packet::consume(vector<Column*> view){
  if (full) assert(false); // should check if it's full before calling!

  for (uint32_t i = 0; i < view.size(); i++)
    if (sentColumns[i])
      offsets[i] += view[i]->transfuse(columns[i] + offsets[i]);
  size += static_cast<size_t>(view[0]->size);

  if (capacity - size < static_cast<size_t>(DEFAULT_CHUNK_SIZE))
    full = true;
}

query::DataPacket* Packet::serialize() {
  query::DataPacket *packet = new query::DataPacket();

  for (uint32_t i = 0; i < types.size(); i++) {
    packet->add_type(abs(types[i]));
    if (sentColumns[i])
      packet->add_data(columns[i], offsets[i]);
    else packet->add_data(string()); // empty string
  }
  return packet;
}

Packet::Packet(const char* data, size_t data_len) {
  // TODO : IMPLEMENT THIS
}

Packet::~Packet() {
  for(uint32_t i = 0; i < columns.size(); i++)
    delete[] columns[i];
}
