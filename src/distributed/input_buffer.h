#ifndef DISTRIBUTED_INPUT_BUFFER_H
#define DISTRIBUTED_INPUT_BUFFER_H

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

using std::queue;
using std::vector;
using std::map;
using std::pair;

class Communication;

class InputBuffer {
  public:
    InputBuffer(Communication *com) : communication(com) {};

    Communication *communication;
    map<int32_t, vector<int32_t> > sources;
    vector<int32_t> source;
    vector<Column*> input;
    vector<int> feeder_state;
    int requested_packets;
    int input_top;

    /** Send data request using network */
    void sendRequest(int provider_stripe, int number, int node);
};

#endif
