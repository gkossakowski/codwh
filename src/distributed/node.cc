#include <vector>
#include <queue>
#include <stdarg.h>

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
  } else {
    data = nei->ReadPacketBlocking(&data_len);
  }
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
  } else if (allow_data && message->has_data_response()) {
    responses.push(message->release_data_response());
  } else {
    debugPrint("ERROR: parseMessage(%s, %b)", message->DebugString().c_str(), allow_data);
    assert(false);
  }

  delete message;
}

void WorkerNode::debugPrint(const char* fmt, ...) {
  va_list ap;
  printf("Worker[%d]: ", nei->my_node_number());
   
  va_start(ap, fmt);
  vfprintf(stdout, fmt, ap);
  va_end(ap);

  printf("\n");
  fflush(stdout);
}

void WorkerNode::getJob() {
  /** Wait until a job occures, than store it in a jobs queue. */
  debugPrint("Awaiting for a job");
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
  debugPrint("Awaiting for data request");
  query::Communication *message;

  while (requests.size() == 0) {
    message = getMessage(true);
    parseMessage(message, true);
  }
  return ;
}

void WorkerNode::getResponse() {
  debugPrint("Awaiting for data response");
  query::Communication *message;

  while (responses.size() == 0) {
    message = getMessage(true);
    parseMessage(message, true);
  }
  return ;
}

void WorkerNode::resetOutput(int buckets) {
  for (uint32_t i = 0; i < output.size(); i++)
    if (output[i].size() != 0) assert(false); // calling during processing is forbidden

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
  debugPrint("parseRequests...");
  int bucket;
  query::DataRequest *request;

  debugPrint("requests.size = %lu", requests.size());
  while (requests.size() > 0) {
    request = requests.back();
    requests.pop();
    if (request->provider_stripe() != stripe) {
      // future stripe, save for later
      assert(request->provider_stripe() > stripe);
      debugPrint("delayed_requests.push(...)");
      delayed_requests.push(request);
      continue;
    }

    bucket = request->consumer_stripe() % output.size();
    debugPrint("request from stripe %d, pending for bucket %d",
        request->consumer_stripe(), bucket);
    consumers_map[bucket] = request->node();
    pending_requests[bucket]++;

    flushBucket(bucket); // try to send data
    delete request;
  }
}

void WorkerNode::packData(vector<Column*> &data, int bucket) {
  queue<NodePacket*> &buck = output[bucket];

  if (buck.size() == 0 || buck.back()->readyToSend)
    buck.push(new NodePacket(data));
  buck.back()->consume(data);

  if (buck.back()->readyToSend)
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
  debugPrint("flushBucket(%d)", bucket);
  if (pending_requests[bucket] == 0) {
    debugPrint("flushBucket: no pending request for bucket");
    return;
  }
  assert(!output[bucket].empty());
  if (!output[bucket].front()->readyToSend) {
    debugPrint("flushBucket: packet not ready to send for bucket");
    return;
  }
  assert(consumers_map[bucket] != -1); // we should know node number from request

  query::Communication com;
  query::DataResponse *response = com.mutable_data_response();
  query::DataPacket *packet;
  const google::protobuf::Reflection *r = response->data().GetReflection();
  string msg;

  response->set_node(nei->my_node_number());
  // send data while we have a full packet and a pending request
  while (pending_requests[bucket] > 0 && output[bucket].front()->readyToSend) {
    packet = output[bucket].front()->serialize();
    output_counters[bucket]++; // increase packet number counter

    response->set_number(output_counters[bucket]); // set number
    r->Swap(response->mutable_data(), packet); // set data
    com.SerializeToString(&msg);
    debugPrint("sending data response");
    nei->SendPacket(consumers_map[bucket], msg.c_str(), msg.size()); // send

    output[bucket].pop(); // remove packet from queue
    pending_requests[bucket]--;
    full_packets--;
    delete packet; // dump packet
  }
}

void WorkerNode::sendRequest(int provider_stripe, int number, int node) {
  string msg;
  query::Communication com;
  com.data_request();

  query::DataRequest *request = com.mutable_data_request();
  request->set_node(nei->my_node_number());
  request->set_provider_stripe(provider_stripe);
  request->set_consumer_stripe(stripe);
  request->set_number(number);

  com.SerializeToString(&msg);
  debugPrint("Sending data request to %d msg={%s}", node, com.DebugString().c_str());
  nei->SendPacket(node, msg.c_str(), msg.size());
}

void WorkerNode::sendEof() {
  query::Communication com;
  query::DataResponse *response = com.mutable_data_response();
  string msg;

  debugPrint("sendEof");

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
  debugPrint("Worker[%d] stripe[%d] job proto tree:\n%s",
         nei->my_node_number(), stripe, op->DebugString().c_str());
  Operation* operation = Factory::createOperation(*op);
  FinalOperation* finalOperation = dynamic_cast<FinalOperation*>(operation);

  if (NULL != finalOperation) {
    int consumeCount = 0;
    int totalConsumeCount = 0;
    while ((consumeCount = finalOperation->consume()) > 0) {
      totalConsumeCount += consumeCount;
      debugPrint("[DATA] consuming %8d (total %8d)", consumeCount, totalConsumeCount); 
    }
  } else {
    vector< vector<Column*> > *buckets;
    int buckets_num = 0;
    int totalPullCount = 0;
    int size;
    do {
      size = 0;
      // pull buckets
      buckets = operation->bucketsPull();
      if (buckets_num == 0) {
        // first iteration; we have to reset output buffer
        buckets_num = buckets->size();
        vector<bool> columns; // TODO: mask unused columns
        resetOutput(buckets_num);
      }

      totalPullCount += (*buckets)[0][0]->size;
      debugPrint("[DATA] pulling %8d (total %8d)", (*buckets)[0][0]->size, totalPullCount); 
      for (int i = 0; i < buckets_num; i++) {
        size += (*buckets)[i][0]->size;
        packData((*buckets)[i], i);
      }
    } while (size > 0);

    // we have all data; try to send it
    assert(buckets_num > 0);
    for (int i = 0; i < buckets_num; i++) {
      if (!output[i].back()->readyToSend) { // close packets
        output[i].back()->readyToSend = true;
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
    std::swap(requests, delayed_requests);
  };
  return ;
}

DataSourceInterface*
WorkerNode::openSourceInterface(int fileId) {
  return nei->OpenDataSourceFile(fileId);
}

DataSinkInterface*
WorkerNode::openSinkInterface() {
  return nei->OpenDataSink();
}


NodePacket::NodePacket(vector<Column*> &view)
  : size(0), readyToSend(false)
{
  size_t row_size = 0;
  columns.resize(view.size(), NULL);
  offsets.resize(view.size(), 0);
  types.resize(view.size(), query::INVALID_TYPE);

  // collect row size and types
  for (uint32_t i = 0; i < view.size(); i++) {
    types[i] = view[i]->getType();
    row_size += global::getTypeSize(types[i]);
  }

  // compute maximum capacity
  capacity = MAX_PACKET_SIZE / row_size;

  // allocate space and reset data counters;
  for (uint32_t i = 0; i < view.size(); i++)
    columns[i] = new char[capacity * global::getTypeSize(types[i])];
}

/** Tries to consume a view. If it's too much data - returns false. */
void NodePacket::consume(vector<Column*> view){
  if (readyToSend) assert(false); // should check if it's full before calling!

  for (uint32_t i = 0; i < view.size(); i++)
    offsets[i] += view[i]->transfuse(columns[i] + offsets[i]);
  size += static_cast<size_t>(view[0]->size);

  if (capacity - size < static_cast<size_t>(DEFAULT_CHUNK_SIZE))
    readyToSend = true;
}

query::DataPacket* NodePacket::serialize() {
  query::DataPacket *packet = new query::DataPacket();

  for (uint32_t i = 0; i < types.size(); i++) {
    packet->add_type(types[i]);
    packet->add_data(columns[i], offsets[i]);
  }
  return packet;
}

NodePacket::NodePacket(const char* data, size_t data_len) {
  // TODO : IMPLEMENT THIS
}

NodePacket::~NodePacket() {
  for(uint32_t i = 0; i < columns.size(); i++)
    delete[] columns[i];
}
