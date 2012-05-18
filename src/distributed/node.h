#ifndef DISTRIBUTED_NODE_H
#define DISTRIBUTED_NODE_H

#include <vector>
#include <queue>

#include "operators/operation.h"
#include "operators/column.h"
#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"

using std::queue;
using std::vector;

/** packet size in bytes */
static const int32_t packet_size = 100 * 1024; // TODO : find out a good value

class WorkerNode {
 private:
  WorkerNode(const WorkerNode &node){};

 protected:
  vector<int32_t> source;
  NodeEnvironmentInterface *nei;
  queue<query::Operation> jobs;
  queue<query::DataRequest> requests;

  /** Input buffer (stack) */
  vector<Column*> input;
  int32_t input_offset;

  // TODO : output buffer

 public:
  WorkerNode(NodeEnvironmentInterface *nei);

  /** Set data sources */
  void setSource(vector<int32_t> source);
  /** Get data from any source */
  vector<Column*> pull(int32_t number);
  /** Pack given data and eventually send to a consumer */
  void packData(vector<Column*> data);
  /** Execute a first plan from a jobs queue */
  void execPlan(query::Operation &op);
  /** Run worker */
  virtual void run();
};

class SchedulerNode : public WorkerNode {
 private:
  SchedulerNode(const SchedulerNode &node);

 public:
  SchedulerNode(NodeEnvironmentInterface *nei, query::Operation query);

  /** Slice query into stripes */
  vector<query::Operation> makeStripes(query::Operation query);
  /** Fill in and send stripes for a given number of nodes */
  void schedule(vector<query::Operation> stripe, int nodes);
  /** Run scheduler */
  virtual void run();
};

#endif // DISTRIBUTED_NODE_H
