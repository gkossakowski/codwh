#ifndef DISTRIBUTED_SCHEDULER_H
#define DISTRIBUTED_SCHEDULER_H

#include <vector>
#include <utility>

#include "node.h"

#include "operators/operation.h"
#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"

using std::vector;
using std::pair;

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
#endif
