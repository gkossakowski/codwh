#include <vector>
#include <queue>

#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"
#include "node.h"
#include "operators/factory.h"

namespace global {
  WorkerNode* worker;
}

WorkerNode::WorkerNode(NodeEnvironmentInterface *nei)
  : communication(nei, &stripe) {
  global::worker = this;
}

int WorkerNode::execPlan(query::Operation *op) {
  communication.debugPrint("Worker[%d] stripe[%d] job proto tree:\n%s",
          communication.nei->my_node_number(), stripe, op->DebugString().c_str());
  Operation* operation = Factory::createOperation(*op);
  FinalOperation* finalOperation = dynamic_cast<FinalOperation*>(operation);

  if (NULL != finalOperation) {
    int consumeCount = 0;
    int totalConsumeCount = 0;
    while ((consumeCount = finalOperation->consume()) > 0) {
      totalConsumeCount += consumeCount;
      communication.debugPrint("[DATA] consuming %8d (total %8d)", consumeCount,
          totalConsumeCount);
    }
  } else {
    vector< vector<Column*> > *buckets;
    int buckets_num = 0;
    int totalPullCount = 0;
    int size;
    do {
      size = 0;
      // pull buckets
      buckets = operation->bucketsPull();
      if (buckets_num == 0) {
        // first iteration; we have to reset output buffer
        buckets_num = buckets->size();
        communication.outputBuffer.resetOutput(buckets_num);
      }

      for (int i = 0; i < buckets_num; i++) {
        size += (*buckets)[i][0]->size;
        communication.outputBuffer.packData((*buckets)[i], i);
      }
      totalPullCount += size;
      communication.debugPrint("[DATA] pulling %8d (total %8d)", size,
                               totalPullCount);
    } while (size > 0);

    // we have all data; try to send it
    assert(buckets_num > 0);
    for (int i = 0; i < buckets_num; i++) {
      if (!communication.outputBuffer.output[i].back()->readyToSend) { // close packets
        communication.outputBuffer.output[i].back()->readyToSend = true;
        communication.outputBuffer.full_packets++;
      }

      // add eof
      communication.debugPrint("[QUEUE] Add eof");
      vector<Column*> noColumns;
      communication.outputBuffer.output[i].push(new NodePacket(noColumns));
      communication.outputBuffer.output[i].back()->readyToSend = true;
      communication.outputBuffer.full_packets++;

      communication.outputBuffer.flushBucket(i);
    }

    // await for requests as long as everything is sent
    while (communication.outputBuffer.full_packets > 0) {
      communication.getRequest();
      communication.outputBuffer.parseRequests();
    }
  }

  delete operation;
  return 0;
}

void WorkerNode::run() {
  int ret = 0;
  while (0 == ret) {
    communication.getJob();
    query::NetworkMessage::Stripe *st;
    st = communication.jobs.front();
    stripe = st->stripe();
    if (communication.delayed_requests.find(stripe) !=
        communication.delayed_requests.end()) {
      communication.requests = communication.delayed_requests[stripe];
      communication.delayed_requests.erase(stripe);
    }
    execPlan(st->mutable_operation());
    communication.jobs.pop();
    assert(communication.requests.empty()); // no pending request after we finish the stripe
  };
  return ;
}
