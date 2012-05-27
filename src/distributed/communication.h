#ifndef DISTRIBUTED_COMMUNICATION_H
#define DISTRIBUTED_COMMUNICATION_H

#include <vector>
#include <queue>
#include <map>
#include <utility>

#include "global.h"
#include "operators/operation.h"
#include "operators/column.h"
#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"
#include "distributed/packet.h"
#include "distributed/input_buffer.h"
#include "distributed/output_buffer.h"

using std::queue;
using std::vector;
using std::map;
using std::pair;

class Communication {
  public:
    Communication(NodeEnvironmentInterface *nei, int *stripe)
      : stripe(stripe), nei(nei), inputBuffer(this), outputBuffer(this) {};

    /** Log debugging messages */
    void debugPrint(const char* str, ...);

    int *stripe;

    NodeEnvironmentInterface *nei;
    InputBuffer inputBuffer;
    OutputBuffer outputBuffer;

    queue<query::NetworkMessage::Stripe *> jobs;
    // queue for data request for current stripe
    queue<query::DataRequest *> requests;
    // key: stripe id -> queue of request for gievn id
    map<int, queue<query::DataRequest *> > delayed_requests;

    queue<query::DataResponse *> responses;

    /** Gets a communication message from network interface */
    query::NetworkMessage* getMessage(bool blocking);
    /** Parses a communication method and stores contained information */
    void parseMessage(query::NetworkMessage *message, bool allow_data);

    /** Wait until any job occurs */
    void getJob();
    /** Wait until any data request occurs */
    void getRequest();
    /** Wait until any data response occurs */
    void getResponse();

    /** Open new DataSourceInterface, caller is responsible for deallocation */
    DataSourceInterface* openSourceInterface(int fileId);
    /** Open sink, should be called exactly one per query */
    DataSinkInterface* openSinkInterface();
};

#endif
