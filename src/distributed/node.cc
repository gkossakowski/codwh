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

void WorkerNode::getJob() {
 /** Wait until a job occures, than store it in a jobs queue. */
 std::cout << "Awaiting for a job" << std::endl;

 query::Communication *message = getMessage(true);
 if (message->has_operation()) {
   jobs.push(message->release_operation());
 } else if (message->has_data_request()) {
   requests.push(message->release_data_request());
 } else assert(false); // there should be no incoming data while awaiting
                       // for a job
 delete message;
 return ;
}

void WorkerNode::setSource(vector<int32_t> source) {
  this->source = source;
  return ;
}

vector<Column*> WorkerNode::pull(int32_t number) {
  // TODO
}

void WorkerNode::packData(vector<Column*> data) {
  // TODO
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
