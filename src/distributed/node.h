#ifndef DISTRIBUTED_NODE_H
#define DISTRIBUTED_NODE_H

#include <vector>
#include <queue>
#include <map>
#include <utility>

#include "operators/operation.h"
#include "operators/column.h"
#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"

using std::queue;
using std::vector;
using std::map;
using std::pair;

/** packet size in bytes */

class Packet;
class WorkerNode;

namespace global {
  extern WorkerNode* worker;
}

class WorkerNode {
  private:
    WorkerNode(const WorkerNode &node){};

  protected:
    NodeEnvironmentInterface *nei;
    queue<query::Communication::Stripe *> jobs;
    queue<query::DataRequest *> requests;
    queue<query::DataRequest *> delayed_requests;

    /** Input buffer */
    map<int32_t, vector<int32_t> > sources;
    vector<int32_t> source;
    vector<Column*> input;
    vector<int> feeder_state;
    int requested_packets;
    int input_top;

    /** Output buffer */
    int full_packets;
    vector< queue<Packet*> > output;
    vector<int> pending_requests;
    vector<int> output_counters;
    vector<int> consumers_map;

    /** Execute a first plan from a jobs queue */
    int execPlan(query::Operation *op);

    /** Reads a data request from queue, tries to satisfy the consumer and
     *  schedule job for later if it's not possible. */
    void parseRequests();

    /** Wait until any job occurs */
    void getJob();
    /** Wait until any data request occurs */
    void getRequest();


    /** Tries to send accumulated data to a consumer */
    void flushBucket(int bucket);
    /** Sends EOF messages to consumers */
    void sendEof();

  public:
    WorkerNode(NodeEnvironmentInterface *nei);
    queue<query::DataResponse *> responses;
    int stripe;

    /** Reset output buffer for a given number of buckets and a boolmask *
     *  of sent columns.                                                 */
    void resetOutput(int buckets);
    /** Pack given data and eventually send to a consumer. Blocks if buffer is *
      * full. */
    void packData(vector<Column*> &data, int bucket);

    /** Run worker */
    virtual void run();

    /** Send message using network */
    void send(query::DataRequest* request);
    /** Wait until any data response occurs */
    void getResponse();

    /** Gets a communication message from network interface */
    query::Communication* getMessage(bool blocking);
    /** Parses a communication method and stores contained information */
    void parseMessage(query::Communication *message, bool allow_data);

    /** Open new DataSourceInterface, caller is responsible for deallocation */
    DataSourceInterface* openSourceInterface(int fileId);
};

class SchedulerNode : public WorkerNode {
  private:
    SchedulerNode(const SchedulerNode &node);
    vector< vector< pair<int, query::Operation> > > nodesJobs;

  protected:
    /** Slice query into stripes */
    vector<query::Operation>* makeStripes(query::Operation query);
    /** Fill in and send stripes for a given number of nodes */
    void schedule(vector<query::Operation> *stripe, uint32_t nodes, int numberOfFiles);
    /** Add job to nodes jobs queue, the given job object is destroyed */
    void sendJob(query::Operation &op, uint32_t node, int stripeId);
    /** Send all jobs to nodes */
    void flushJobs();

   public:
    SchedulerNode(NodeEnvironmentInterface *nei) : WorkerNode(nei) {
      nodesJobs.resize(nei->nodes_count());
    };

    /** Run scheduler */
    virtual void run(const query::Operation &op);
};

class Packet {
  private:
    vector<char *> columns;
    vector<uint32_t> offsets;
    vector<query::ColumnType> types;
    size_t size; /** size in rows */
    size_t capacity; /** maximum capacity in rows */

  public:
    bool full; /** only Packet should write to this! */
    Packet(vector<Column*> &view);
    Packet(const char* data, size_t data_len);

    void consume(vector<Column*> view);
    query::DataPacket* serialize();

    ~Packet();
};

#endif // DISTRIBUTED_NODE_H
