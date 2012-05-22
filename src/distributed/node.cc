#include <vector>
#include <queue>

#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"
#include "node.h"

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
 // TODO
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

 if (buck.back()->full)
  buck.push(new Packet(data, &sent_columns));
 buck.back()->consume(data);

 if (buck.back()->full)
  full_packets++;
 assert(full_packets <= MAX_OUTPUT_PACKETS);

 flushBucket(bucket);

 while (full_packets == MAX_OUTPUT_PACKETS) {
  while (requests.size() == 0) {
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
 string msg;
 response.set_node(nei->my_node_number());

 while (pending_requests[bucket] > 0 && !output[bucket].front()->full) {
  size_t data_len = output[bucket].front()->serialize(output_data);
  output_counters[bucket]++;

  response.set_number(output_counters[bucket]);
  response.set_size(data_len);
  response.set_data(output_data, data_len);
  response.SerializeToString(&msg);

  nei->SendPacket(bucket, msg.c_str(), msg.size());
 }
}

int WorkerNode::execPlan(query::Operation *op) {
 std::cout << "Worker job proto tree: " << op->DebugString() << "\n";

 // TODO : IMPLEMENT THIS
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

vector<query::Operation>* SchedulerNode::makeStripes(query::Operation query) {
 // TODO : IMPLEMENT THIS
 vector<query::Operation> *stripes = new vector<query::Operation>();
 stripes->push_back(query);
 return stripes;
}

void SchedulerNode::schedule(vector<query::Operation> *stripe, uint32_t nodes) {
 // TODO : IMPLEMENT THIS
 for (uint32_t i = 1; i <= nodes; i++)
  sendJob((*stripe)[0], i);
 return ;
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
 vector<query::Operation> *stripes = makeStripes(op);
 schedule(stripes, nei->nodes_count() - 1);
 delete stripes;

 // TODO : switch to a worker mode
 // execPlan(finalOperation);
 return ;
}


Packet::Packet(vector<Column*> &view, vector<bool> *sent_columns)
  : sent_columns(sent_columns), full(false)
{
 size_t row_size = 0;
 columns.resize(view.size(), NULL);
 offsets.resize(view.size(), 0);
 types.resize(view.size(), 0);

 // collect row size and types
 for (uint32_t i = 0; i < view.size(); i++)
  if ((*sent_columns)[i]){
    types[i] = view[i]->getType();
    row_size += global::TypeSize[types[i]];
  }

 // compute maximum capacity
 capacity = MAX_PACKET_SIZE / row_size;

 // allocate space and reset data counters;
 for (uint32_t i = 0; i < view.size(); i++)
  if ((*sent_columns)[i]) {
   columns[i] = new char[capacity * global::TypeSize[types[i]]];
  }
}

/** Tries to consume a view. If it's too much data - returns false. */
void Packet::consume(vector<Column*> view){
 if (full) assert(false); // should check if it's full before calling!

 size_t delta;
 for (uint32_t i = 0; i < view.size(); i++)
  if ((*sent_columns)[i]) {
   delta = view[i]->transfuse(columns[i] + offsets[i]);
   offsets[i] += delta;
   size += delta;
  }

 if (capacity - size < static_cast<size_t>(DEFAULT_CHUNK_SIZE))
  full = true;
}

size_t Packet::serialize(char *buffer) {
 // TODO : It's vulenrable to a big number of columns (not sufficient space for
 // metadata).

 // write number of columns
 size_t offset = 0;
 *(reinterpret_cast<uint32_t*>(buffer)) = columns.size();
 // write columns mask
 std::copy(sent_columns->begin(), sent_columns->end(), buffer + offset);
 // write offsets
 std::copy(offsets.begin(), offsets.end(), buffer + offset);
 // write types
 std::copy(types.begin(), types.end(), buffer + offset);
 offset = sizeof(uint32_t) + (sizeof(bool) + sizeof(uint32_t) * 2) * columns.size();
 // write columns
 for (uint32_t i = 0; i < columns.size(); i++) {
  std::copy(columns[i], columns[i] + offsets[i], buffer + offset);
  offset += offsets[i];
 }

 return offset;
}

Packet::Packet(char data[], size_t data_len) {
 // TODO : IMPLEMENT THIS
}

Packet::~Packet() {
 for(uint32_t i = 0; i < columns.size(); i++)
  delete[] columns[i];
}
