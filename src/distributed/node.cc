#include <vector>
#include <queue>

#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"
#include "node.h"

WorkerNode::WorkerNode(NodeEnvironmentInterface *nei) : nei(nei) {
 // TODO
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

void WorkerNode::execPlan(query::Operation &op) {
  // TODO
}

void WorkerNode::run() {
  // TODO
}


SchedulerNode::SchedulerNode(NodeEnvironmentInterface *nei,
                             query::Operation query) : WorkerNode(nei)
{
  // TODO
}

vector<query::Operation> SchedulerNode::makeStripes(query::Operation query) {
  // TODO
}

void SchedulerNode::schedule(vector<query::Operation> stripe, int nodes) {
  // TODO
}

void SchedulerNode::run() {
  // TODO
}
