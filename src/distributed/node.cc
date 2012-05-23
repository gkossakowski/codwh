#include <vector>
#include <queue>

#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"
#include "node.h"
#include "../operators/factory.h"

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
 if (message->has_operation()) {
   jobs.push(message->release_operation());
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

void WorkerNode::setSource(vector<int32_t> source) {
 this->source = source;
}

vector<Column*> WorkerNode::pull(int32_t number) {
  // TODO: implement
  assert(false); // not implemented yet
  return vector<Column*>();
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
 full_packets = 0;
}

void WorkerNode::packData(vector<Column*> &data, int bucket) {
 queue<Packet*> &buck = output[bucket];
 query::DataRequest *request;
 int buckets_num = output.size();

 if (buck.size() == 0 || buck.back()->full)
  buck.push(new Packet(data, sent_columns));
 buck.back()->consume(data);

 if (buck.back()->full)
  full_packets++;
 assert(full_packets <= MAX_OUTPUT_PACKETS);

 flushBucket(bucket);

 /* Probably, the situation below won't occur. */
 while (full_packets == MAX_OUTPUT_PACKETS) {
  while (requests.size() > 0) {
   request = requests.back();
   requests.pop();

   int i = request->node() % buckets_num;
   pending_requests[i] += request->number();
   flushBucket(i); // try to send data

   delete request;
  }
  getRequest();
 }
}

void WorkerNode::flushBucket(int bucket) {
 if (pending_requests[bucket] == 0 || output[bucket].front()->full)
  return;

 query::DataResponse response;
 query::DataPacket *packet;
 const google::protobuf::Reflection *r = response.data().GetReflection();
 string msg;
 response.set_node(nei->my_node_number());

 while (pending_requests[bucket] > 0 && !output[bucket].front()->full) {
  packet = output[bucket].front()->serialize();
  output_counters[bucket]++;

  response.set_number(output_counters[bucket]);
  r->Swap(response.mutable_data(), packet);
  response.SerializeToString(&msg);

  nei->SendPacket(bucket, msg.c_str(), msg.size());
  delete packet;
 }
}

int WorkerNode::execPlan(query::Operation *op) {
  printf("Worker[%d] job proto tree:\n%s\n",
      nei->my_node_number(), op->DebugString().c_str());
  Operation* operation = Factory::createOperation(*op);
  FinalOperation* finalOperation = dynamic_cast<FinalOperation*>(operation);

  if (NULL != finalOperation) {
    while (finalOperation->consume() > 0) {
    }
  } else {
    vector<Column*>* data;
    do {
      data = operation->pull();
      // TODO: IMPLEMENT waiting queue
    } while ((*data)[0]->size > 0);
  }

  delete operation;
  return 0;
}

void WorkerNode::run() {
 int ret = 0;
 while (0 == ret) {
  if (0 == jobs.size()) getJob();
  execPlan(jobs.front());
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
    stripes.push_back(op);
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
    // types of all columns of the source of group by
    vector<int> types;
    {
      GroupByOperation* groupByOp = static_cast<GroupByOperation*>(Factory::createOperation(op));
      columns = groupByOp->getUsedColumnsId();
      types = Factory::createOperation(groupBy.source())->getTypes();
    }

    query::Operation shuffle;
    shuffle.mutable_shuffle()->mutable_source()->MergeFrom(lastStripe);
    for (unsigned int i=0; i < columns.size(); i++) {
      shuffle.mutable_shuffle()->add_column(columns[i]);
      switch (types[columns[i]]) {
        case query::ScanOperation_Type_BOOL:
          shuffle.mutable_shuffle()->add_type(query::BOOL);
          break;
        case query::ScanOperation_Type_INT:
          shuffle.mutable_shuffle()->add_type(query::INT);
          break;
        case query::ScanOperation_Type_DOUBLE:
          shuffle.mutable_shuffle()->add_type(query::DOUBLE);
          break;
        default:
          break;
      }
    }
    stripes.push_back(shuffle);

    groupBy.clear_source();
    query::UnionOperation* union_ = groupBy.mutable_source()->mutable_union_();
    for (unsigned i = 0; i < columns.size(); i++) {
      union_->add_column(columns[i]);
      switch (types[columns[i]]) {
        case query::ScanOperation_Type_BOOL:
          union_->add_type(query::BOOL);
          break;
        case query::ScanOperation_Type_INT:
          union_->add_type(query::INT);
          break;
        case query::ScanOperation_Type_DOUBLE:
          union_->add_type(query::DOUBLE);
          break;
        default:
          break;
      }
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

vector<int> getColumnTypes(const query::Operation& opProto) {
  Operation* op = Factory::createOperation(opProto);
  vector<int> result = op->getTypes();
  return result;
}

void addColumnsAndTypesToShuffle(query::ShuffleOperation& shuffle) {
  vector<int> types = getColumnTypes(shuffle.source());
  for (unsigned int i = 0; i < types.size(); i++) {
    shuffle.add_column(i);
    // TODO establish one message for types that we'll use everywhere
    // so we can get rid of those tiring conversions
    switch (types[i]) {
      case query::ScanOperation_Type_BOOL:
        shuffle.add_type(query::BOOL);
        break;
      case query::ScanOperation_Type_INT:
        shuffle.add_type(query::INT);
        break;
      case query::ScanOperation_Type_DOUBLE:
        shuffle.add_type(query::DOUBLE);
        break;
      default:
        assert(false);
    }
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
void assignNodesToUnion(query::Operation& stripe, int nodeStart, int nodeEnd) {
  if (stripe.has_scan()) {
    // stripes should never have plain scan
    assert(false);
  } else if (stripe.has_compute()) {
    assignNodesToUnion(*stripe.mutable_compute()->mutable_source(), nodeStart, nodeEnd);
  } else if (stripe.has_filter()) {
    assignNodesToUnion(*stripe.mutable_filter()->mutable_source(), nodeStart, nodeEnd);
  } else if (stripe.has_group_by()) {
    assignNodesToUnion(*stripe.mutable_group_by()->mutable_source(), nodeStart, nodeEnd);
  } else if (stripe.has_scan_own()) {
    // only the first stripe can have scan own operation and this method
    // should not be called for the first stripe
  } else if (stripe.has_shuffle()) {
    assignNodesToUnion(*stripe.mutable_shuffle()->mutable_source(), nodeStart, nodeEnd);
  } else if (stripe.has_union_()) {
    for (int i=nodeStart; i<nodeEnd; i++) {
      stripe.mutable_union_()->add_source(i);
    }
  } else if (stripe.has_final()) {
    assignNodesToUnion(*stripe.mutable_final()->mutable_source(), nodeStart, nodeEnd);
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

void SchedulerNode::schedule(vector<query::Operation> *stripes, uint32_t nodes) {
  int nodesPerStripe = nodes / stripes->size();
  // we do not hamdle situation when nodes < stripes->size() yet
  assert(nodesPerStripe > 0);
  int nodeStart = 1;
  int nodeEnd = nodeStart + nodesPerStripe;
  // process the first stripe
  {
    query::Operation& firstStripe = stripes->front();
    assignReceiversCount(firstStripe, nodesPerStripe);
    for (int i = nodeStart; i < nodeEnd; i++) {
      sendJob(firstStripe, i);
    }
  }
  for (unsigned i = 1; i + 1 < stripes->size(); i++) {
    query::Operation& stripe = (*stripes)[i];
    // assign nodes to union that were sent jobs to
    // in a previous iteration
    assignNodesToUnion(stripe, nodeStart, nodeEnd);
    nodeStart = 1+i*nodesPerStripe;
    nodeEnd = 1+(i+1)*nodesPerStripe;
    assignReceiversCount(stripe, nodesPerStripe);
    for (int j = nodeStart; j < nodeEnd; i++) {
      sendJob(stripe, j);
    }
  }
  // schedule the last stripe, use all the remaining workers
  {
    query::Operation& lastStripe = stripes->back();
    assignNodesToUnion(lastStripe, nodeStart, nodeEnd);
    nodeStart = nodeEnd;
    nodeEnd = nodes+1;
    for (int i = nodeStart; i < nodeEnd; i++) {
      sendJob(lastStripe, i);
    }
  }
}

void SchedulerNode::sendJob(query::Operation &op, uint32_t node) {
 query::Communication com;
 const google::protobuf::Reflection *r = com.operation().GetReflection();
 string msg;

 r->Swap(com.mutable_operation(), &op); // breaks `op`, but no need to repair
 com.SerializeToString(&msg);
 nei->SendPacket(node, msg.c_str(), msg.size());
 return ;
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
  schedule(stripes, nei->nodes_count() - 1);
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
 types.resize(view.size(), 0);

 // collect row size and types
 for (uint32_t i = 0; i < view.size(); i++) {
  types[i] = view[i]->getType();
  if (sent_columns[i]){
    row_size += global::TypeSize[types[i]];
  } else types[i] *= -1; // omited column
 }

 // compute maximum capacity
 capacity = MAX_PACKET_SIZE / row_size;

 // allocate space and reset data counters;
 for (uint32_t i = 0; i < view.size(); i++)
  if (sent_columns[i])
   columns[i] = new char[capacity * global::TypeSize[types[i]]];
}

/** Tries to consume a view. If it's too much data - returns false. */
void Packet::consume(vector<Column*> view){
 if (full) assert(false); // should check if it's full before calling!

 for (uint32_t i = 0; i < view.size(); i++)
  if (types[i] > 0)
   offsets[i] += view[i]->transfuse(columns[i] + offsets[i]);
 size += static_cast<size_t>(view[0]->size);

 if (capacity - size < static_cast<size_t>(DEFAULT_CHUNK_SIZE))
  full = true;
}

query::DataPacket* Packet::serialize() {
 query::DataPacket *packet = new query::DataPacket();

 for (uint32_t i = 0; i < types.size(); i++) {
  packet->add_type(abs(types[i]));
  if (types[i] > 0)
   packet->add_data(columns[i], offsets[i]);
  else packet->add_data(string()); // empty string
 }
 return packet;
}

Packet::Packet(char data[], size_t data_len) {
 // TODO : IMPLEMENT THIS
}

Packet::~Packet() {
 for(uint32_t i = 0; i < columns.size(); i++)
  delete[] columns[i];
}
