#include <vector>
#include <queue>

#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"
#include "node.h"

query::Communication* WorkerNode::getMessage(bool blocking) {
 char *data;
 size_t data_len;
 query::Communication *message;

 if (!blocking) {
  data = nei->ReadPacketNotBlocking(&data_len);
  if (NULL == data)
   return NULL;
 }
 data = nei->ReadPacketBlocking(&data_len);
 message = new query::Communication();
 message->ParseFromString(string(data, data_len));
 delete[] data;

 return message;
}

void WorkerNode::getJob() {
 /** Wait until a job occures, than store it in a jobs queue. */
 std::cout << "Awaiting for a job" << std::endl;

 query::Communication *message = getMessage(true);
 if (message->has_operation()) {
   jobs.push(message->release_operation());
 } else if (message->has_data_request()) {
   requests.push(message->release_data_request());
 } else assert(false); // there should be no incoming data while awaiting
                       // for a job
 delete message;
 return ;
}

void WorkerNode::setSource(vector<int32_t> source) {
  this->source = source;
  return ;
}

vector<Column*> WorkerNode::pull(int32_t number) {
  // TODO: implement
  assert(false); // not implemented yet
  return vector<Column*>();
}

void WorkerNode::packData(vector<Column*> data) {
  // TODO
}

int WorkerNode::execPlan(query::Operation *op) {
  printf("Worker[%d] job proto tree:\n%s\n",
      nei->my_node_number(), op->DebugString().c_str());
  Operation* operation = Factory::createOperation(*op);
  FinalOperation* finalOperation = dynamic_cast<FinalOperation*>(operation);

  if (NULL != finalOperation) {
    while (finalOperation->consume() > 0) {
    }
  } else {
    vector<Column*>* data;
    do {
      data = operation->pull();
      // TODO: IMPLEMENT waiting queue
    } while ((*data)[0]->size > 0);
  }

  delete operation;
  return 0;
}

void WorkerNode::run() {
 int ret = 0;
 while (0 == ret) {
  if (0 == jobs.size()) getJob();
  execPlan(jobs.front());
  jobs.pop();
 };
 return ;
}
/*
 * Cuts query into stripes.
 *
 * Each stripe is created whenever there's group by operation
 * encountered in query tree.
 *
 * Each stripe (apart from the first and the last ones) has union
 * operation as a source and shuffle operation as a sink.
 * Two consecutive stripes should have column fields matching in
 * shuffle and union operations (so they can be plugged together).
 *
 * When Shuffle and Union operations are introduced their fields
 * respectively `receiversCount` and `source` are left uninitialized.
 * It's job of the other part of the scheduler to initialize them.
 */
vector<query::Operation> stripeOperation(const query::Operation query) {
  vector<query::Operation> stripes;
  query::Operation op = query;
  if (query.has_scan()) {
    stripes.push_back(op);
  } else if (query.has_compute()) {
    stripes = stripeOperation(query.compute().source());
    query::Operation lastStripe = stripes.back();
    stripes.pop_back();
    op.mutable_compute()->clear_source();
    op.mutable_compute()->mutable_source()->MergeFrom(lastStripe);
    stripes.push_back(op);
  } else if (query.has_filter()) {
    stripes = stripeOperation(query.filter().source());
    query::Operation lastStripe = stripes.back();
    stripes.pop_back();
    op.mutable_filter()->clear_source();
    op.mutable_filter()->mutable_source()->MergeFrom(lastStripe);
    stripes.push_back(op);
  } else if (query.has_group_by()) {
    stripes = stripeOperation(query.group_by().source());
    query::Operation lastStripe = stripes.back();
    stripes.pop_back();

    query::GroupByOperation groupBy = query.group_by();

    // columns needed by group by operations both for keys and values
    vector<int> columns;
    for (int i = 0; i < groupBy.group_by_column_size(); i++) {
      columns.push_back(groupBy.group_by_column(i));
    }
    for (int i = 0; i < groupBy.aggregations_size(); i++) {
      if (groupBy.aggregations(i).has_aggregated_column())
        columns.push_back(groupBy.aggregations(i).aggregated_column());
    }

    query::Operation shuffle;
    shuffle.mutable_shuffle()->mutable_source()->MergeFrom(lastStripe);
    for (unsigned int i=0; i < columns.size(); i++) {
      shuffle.mutable_shuffle()->add_column(columns[i]);
    }
    stripes.push_back(shuffle);

    groupBy.clear_source();
    query::UnionOperation* union_ = groupBy.mutable_source()->mutable_union_();
    for (unsigned i = 0; i < columns.size(); i++) {
      union_->add_column(columns[i]);
    }

    query::Operation groupByOp;
    groupByOp.mutable_group_by()->MergeFrom(groupBy);
    stripes.push_back(groupByOp);
  } else if (query.has_scan_own() || query.has_union_() || query.has_shuffle() || query.has_final()) {
    // this should never happen as those nodes are introduced by stripeOperation
    assert(false);
  }
  return stripes;
}

vector<query::Operation>* SchedulerNode::makeStripes(query::Operation query) {
  vector<query::Operation> stripes = stripeOperation(query);
  vector<query::Operation> *stripes_ptr = new vector<query::Operation>(stripes.begin(), stripes.end());
  return stripes_ptr;
}

/*
 * Assigns nodes to union operation that is stored in a given stripe. This
 * method should not be called with first stripe that has no union operations.
 *
 * Nodes are specified as a range [nodeStart, nodeEnd).
 *
 * This method mutates passed stripe.
 */
void assignNodesToUnion(query::Operation& stripe, int nodeStart, int nodeEnd) {
  if (stripe.has_scan()) {
    // stripes should never have plain scan
    assert(false);
  } else if (stripe.has_compute()) {
    assignNodesToUnion(*stripe.mutable_compute()->mutable_source(), nodeStart, nodeEnd);
  } else if (stripe.has_filter()) {
    assignNodesToUnion(*stripe.mutable_filter()->mutable_source(), nodeStart, nodeEnd);
  } else if (stripe.has_group_by()) {
    assignNodesToUnion(*stripe.mutable_group_by()->mutable_source(), nodeStart, nodeEnd);
  } else if (stripe.has_scan_own()) {
    // only the first stripe can have scan own operation and this method
    // should not be called for the first stripe
  } else if (stripe.has_shuffle()) {
    assignNodesToUnion(*stripe.mutable_shuffle()->mutable_source(), nodeStart, nodeEnd);
  } else if (stripe.has_union_()) {
    for (int i=nodeStart; i<nodeEnd; i++) {
      stripe.mutable_union_()->add_source(i);
    }
  } else if (stripe.has_final()) {
    assignNodesToUnion(*stripe.mutable_final()->mutable_source(), nodeStart, nodeEnd);
  }
}

void assignReceiversCount(query::Operation& stripe, int receiversCount) {
  // the last operation (the root of stripe) should be shuffle
  assert(stripe.has_shuffle());
  stripe.mutable_shuffle()->set_receiverscount(receiversCount);
}

void SchedulerNode::schedule(vector<query::Operation> *stripes, uint32_t nodes) {
  int nodesPerStripe = nodes / stripes->size();
  // we do not hamdle situation when nodes < stripes->size() yet
  assert(nodesPerStripe > 0);
  int nodeStart = 1;
  int nodeEnd = nodeStart + nodesPerStripe;
  // process the first stripe
  {
    query::Operation& firstStripe = stripes->front();
    assignReceiversCount(firstStripe, nodesPerStripe);
    for (int i = nodeStart; i < nodeEnd; i++) {
      sendJob(firstStripe, i);
    }
  }
  for (unsigned i = 1; i + 1 < stripes->size(); i++) {
    query::Operation& stripe = (*stripes)[i];
    // assign nodes to union that were sent jobs to
    // in a previous iteration
    assignNodesToUnion(stripe, nodeStart, nodeEnd);
    nodeStart = 1+i*nodesPerStripe;
    nodeEnd = 1+(i+1)*nodesPerStripe;
    assignReceiversCount(stripe, nodesPerStripe);
    for (int j = nodeStart; j < nodeEnd; i++) {
      sendJob(stripe, j);
    }
  }
  // schedule the last stripe, use all the remaining workers
  {
    query::Operation& lastStripe = stripes->back();
    assignNodesToUnion(lastStripe, nodeStart, nodeEnd);
    nodeStart = nodeEnd;
    nodeEnd = nodes+1;
    for (int i = nodeStart; i < nodeEnd; i++) {
      sendJob(lastStripe, i);
    }
  }
}

void SchedulerNode::sendJob(query::Operation &op, uint32_t node) {
 query::Communication com;
 const google::protobuf::Reflection *r = com.operation().GetReflection();
 string msg;

 r->Swap(com.mutable_operation(), &op); // breaks `op`, but no need to repair
 com.SerializeToString(&msg);
 nei->SendPacket(node, msg.c_str(), msg.size());
 return ;
}

void SchedulerNode::run(const query::Operation &op) {
 std::cout << "Scheduling proto: " << op.DebugString() << "\n";
 vector<query::Operation> *stripes = makeStripes(op);
  std::cout << "after striping: " << std::endl;
  for (unsigned i=0; i < stripes->size() ; i++) {
    std::cout << "STRIPE " << i << std::endl;
    std::cout << (*stripes)[i].DebugString() << std::endl;
  }
 schedule(stripes, nei->nodes_count() - 1);
 delete stripes;

 // TODO : switch to a worker mode
 // execPlan(finalOperation);
 return ;
}
