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

class Packet;

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

  /** Output buffer */
  int full_packets;
  char *output_data;
  vector< queue<Packet*> > output;
  vector<int> pending_requests;
  vector<int> output_counters;
  vector<bool> sent_columns;

  /** Execute a first plan from a jobs queue */
  int execPlan(query::Operation *op);

  /** Gets a communication message from network interface */
  query::Communication* getMessage(bool blocking);
  /** Parses a communication method and stores contained information */
  void parseMessage(query::Communication *message, bool allow_data);

  /** Wait until any job occurs */
  void getJob();
  /** Wait until any data request occurs */
  void getRequest();

 public:
  WorkerNode(NodeEnvironmentInterface *nei)
  : nei(nei), output_data(new char[MAX_PACKET_SIZE + 100]) {};

  /** Set data sources */
  void setSource(vector<int32_t> source);
  /** Get data from any source */
  vector<Column*> pull(int32_t number);

  /** Reset output buffer for a given number of buckets and a boolmask *
   *  of sent columns.                                                 */
  void resetOutput(int buckets, vector<bool> &columns);
  /** Pack given data and eventually send to a consumer. Blocks if buffer is *
    * full.                                                                  */
  void packData(vector<Column*> &data, int bucket);
  /** Tries to send accumulated data to a consumer */
  void flushBucket(int bucket);

  /** Run worker */
  virtual void run();

  ~WorkerNode(){
    delete[] output_data;
  }
};

class SchedulerNode : public WorkerNode {
 private:
  SchedulerNode(const SchedulerNode &node);

 protected:
  /** Slice query into stripes */
  vector<query::Operation>* makeStripes(query::Operation query);
  /** Fill in and send stripes for a given number of nodes */
  void schedule(vector<query::Operation> *stripe, uint32_t nodes);
  /** Send job to a given node, the given job object is destroyed */
  void sendJob(query::Operation &op, uint32_t node);

 public:
  SchedulerNode(NodeEnvironmentInterface *nei) : WorkerNode(nei) {};

  /** Run scheduler */
  virtual void run(const query::Operation &op);
};

class Packet {
 private:
  vector<char *> columns;
  vector<uint32_t> offsets;
  vector<int32_t> types;
  size_t size; /** size in rows */
  size_t capacity; /** maximum capacity in rows */

 public:
  bool full; /** only Packet should write to this! */
  Packet(vector<Column*> &view, vector<bool> &sent_columns);
  Packet(char data[], size_t data_len);

  void consume(vector<Column*> view);
  query::DataPacket* serialize();

  ~Packet();
};

#endif // DISTRIBUTED_NODE_H
