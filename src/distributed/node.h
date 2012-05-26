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

class NodePacket;
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
    // queue for data request for current stripe
    queue<query::DataRequest *> requests;
    // key: stripe id -> queue of request for gievn id
    map<int, queue<query::DataRequest *> > delayed_requests;

    /** Input buffer */
    map<int32_t, vector<int32_t> > sources;
    vector<int32_t> source;
    vector<Column*> input;
    vector<int> feeder_state;
    int requested_packets;
    int input_top;

    /** Output buffer */
    int full_packets;
    /** Output packets per bucket */
    vector< queue<NodePacket*> > output;
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
  
    void debugPrint(const char* str, ...);

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

    /** Send data request using network */
    void sendRequest(int provider_stripe, int number, int node);
    /** Wait until any data response occurs */
    void getResponse();

    /** Gets a communication message from network interface */
    query::Communication* getMessage(bool blocking);
    /** Parses a communication method and stores contained information */
    void parseMessage(query::Communication *message, bool allow_data);

    /** Open new DataSourceInterface, caller is responsible for deallocation */
    DataSourceInterface* openSourceInterface(int fileId);
    /** Open sink, should be called exactly one per query */
    DataSinkInterface* openSinkInterface();
};

class NodePacket {
  private:
    vector<char *> columns;
    vector<uint32_t> offsets;
    vector<query::ColumnType> types;
    size_t size; /** size in rows */
    size_t capacity; /** maximum capacity in rows */

  public:
    bool readyToSend; /** only Packet should write to this! */
    NodePacket(vector<Column*> &view);
    NodePacket(const char* data, size_t data_len);

    void consume(vector<Column*> view);
    query::DataPacket* serialize();

    ~NodePacket();
};

#endif // DISTRIBUTED_NODE_H
