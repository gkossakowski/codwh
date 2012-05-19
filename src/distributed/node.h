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
  queue<query::Operation *> jobs;
  queue<query::DataRequest *> requests;

  /** Input buffer (stack) */
  vector<Column*> input;
  int32_t input_offset;

  // TODO : output buffer

  /** Execute a first plan from a jobs queue */
  int execPlan(query::Operation *op);

  query::Communication* getMessage(bool blocking);

  /** Wait until any job occurs */
  void getJob();

 public:
  WorkerNode(NodeEnvironmentInterface *nei) : nei(nei) {};

  /** Set data sources */
  void setSource(vector<int32_t> source);
  /** Get data from any source */
  vector<Column*> pull(int32_t number);
  /** Pack given data and eventually send to a consumer */
  void packData(vector<Column*> data);
  /** Run worker */
  virtual void run();
};

class SchedulerNode : public WorkerNode {
 private:
  SchedulerNode(const SchedulerNode &node);

 protected:
  /** Slice query into stripes */
  vector<query::Operation>* makeStripes(query::Operation query);
  /** Fill in and send stripes for a given number of nodes */
  void schedule(vector<query::Operation> *stripe, uint32_t nodes);
  /** Send job to a given node */
  void sendJob(const query::Operation &op, uint32_t node);

 public:
  SchedulerNode(NodeEnvironmentInterface *nei) : WorkerNode(nei) {};

  /** Run scheduler */
  virtual void run(const query::Operation &op);
};

#endif // DISTRIBUTED_NODE_H
