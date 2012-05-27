#ifndef DISTRIBUTED_PACKET_H
#define DISTRIBUTED_PACKET_H

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


using std::vector;

class NodePacket {
  private:
    vector<char *> columns;
    vector<uint32_t> offsets;
    vector<query::ColumnType> types;
    size_t size; /** size in rows */
    size_t capacity; /** maximum capacity in rows */

  public:
    bool readyToSend; /** only Packet should write to this! */
    // To create EOF past empty vector
    NodePacket(vector<Column*> &view);
    bool isEOF() {
      return columns.empty();
    }

    void consume(vector<Column*> view);
    query::DataPacket* serialize();
    virtual ~NodePacket();
};

#endif
