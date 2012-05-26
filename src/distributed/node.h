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
#include "distributed/communication.h"
#include "distributed/input_buffer.h"
#include "distributed/output_buffer.h"

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
  protected:
    int stripe;
    /** Execute a first plan from a jobs queue */
    int execPlan(query::Operation *op);

  public:
    Communication communication;
    WorkerNode(NodeEnvironmentInterface *nei);

    /** Run worker */
    virtual void run();
};

#endif // DISTRIBUTED_NODE_H
