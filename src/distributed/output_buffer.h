#ifndef DISTRIBUTED_OUTPUT_BUFFER_H
#define DISTRIBUTED_OUTPUT_BUFFER_H

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

class OutputBuffer {
  public:
    OutputBuffer(Communication *com) : communication(com) {};

    Communication *communication;
    /** Output buffer */
    int full_packets;
    /** Output packets per bucket */
    vector< queue<NodePacket*> > output;
    vector<int> pending_requests;
    vector<int> output_counters;
    vector<int> consumers_map;

    /** Reads a data request from queue, tries to satisfy the consumer and
     *  schedule job for later if it's not possible. */
    void parseRequests();
    /** Reset output buffer for a given number of buckets and a boolmask *
     *  of sent columns.                                                 */
    void resetOutput(int buckets);
    /** Pack given data and eventually send to a consumer. Blocks if buffer is *
      * full. */
    void packData(vector<Column*> &data, int bucket);

    /** Tries to send accumulated data to a consumer */
    void flushBucket(int bucket);
    /** Sends EOF messages to consumers */
    void sendEof();
};

#endif
