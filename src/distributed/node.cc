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
  } else if (allow_data && message->has_data_response()) {
    responses.push(message->release_data_response());
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

void WorkerNode::getResponse() {
  std::cout << "Awaiting for data request" << std::endl;
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
  int bucket;
  query::DataRequest *request;

  while (requests.size() > 0) {
    request = requests.back();
    requests.pop();
    if (request->provider_stripe() != stripe) {
      // future stripe, save for later
      assert(request->provider_stripe() > stripe);
      delayed_requests.push(request);
      continue;
    }

    bucket = request->consumer_stripe() % output.size();
    consumers_map[bucket] = request->node();
    pending_requests[bucket] += request->number();

    flushBucket(bucket); // try to send data
    delete request;
  }
}

void WorkerNode::packData(vector<Column*> &data, int bucket) {
  queue<Packet*> &buck = output[bucket];

  if (buck.size() == 0 || buck.back()->full)
    buck.push(new Packet(data));
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
  printf("Worker[%d] stripe[%d] job proto tree:\n%s",
         nei->my_node_number(), stripe, op->DebugString().c_str());
  std::cout << std::endl; // ugly buffer flush
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
        resetOutput(buckets_num);
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
    std::swap(requests, delayed_requests);
  };
  return ;
}

DataSourceInterface*
WorkerNode::openSourceInterface(int fileId) {
  return nei->OpenDataSourceFile(fileId);
}



Packet::Packet(vector<Column*> &view)
  : size(0), full(false)
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
void Packet::consume(vector<Column*> view){
  if (full) assert(false); // should check if it's full before calling!

  for (uint32_t i = 0; i < view.size(); i++)
    offsets[i] += view[i]->transfuse(columns[i] + offsets[i]);
  size += static_cast<size_t>(view[0]->size);

  if (capacity - size < static_cast<size_t>(DEFAULT_CHUNK_SIZE))
    full = true;
}

query::DataPacket* Packet::serialize() {
  query::DataPacket *packet = new query::DataPacket();

  for (uint32_t i = 0; i < types.size(); i++) {
    packet->add_type(types[i]);
    packet->add_data(columns[i], offsets[i]);
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
