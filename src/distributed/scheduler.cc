#include <queue>

#include "proto/operations.pb.h"
#include "node_environment/node_environment.h"
#include "operators/factory.h"

#include "scheduler.h"

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
    query::Operation scanOwnOp;
    query::ScanFileOperation& scanOwn = *scanOwnOp.mutable_scan_file();
    for (int i = 0; i < query.scan().column_size(); i++) {
      scanOwn.add_column(query.scan().column(i));
      switch (query.scan().type(i)) {
        case query::ScanOperation_Type_BOOL:
          scanOwn.add_type(query::BOOL);
          break;
        case query::ScanOperation_Type_INT:
          scanOwn.add_type(query::INT);
          break;
        case query::ScanOperation_Type_DOUBLE:
          scanOwn.add_type(query::DOUBLE);
          break;
        default:
          assert(false);
      }
    }
    stripes.push_back(scanOwnOp);
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
    // columns used for hashing, subset of `columns`
    vector<int> keyColumns;
    // types of all columns of the source of group by
    vector<query::ColumnType> types;
    {
      GroupByOperation* groupByOp = static_cast<GroupByOperation*>(Factory::createOperation(op));
      columns = groupByOp->getUsedColumnsId();
      keyColumns = groupByOp->getKeyColumnsId();
      types = Factory::createOperation(groupBy.source())->getTypes();
    }
    
    query::Operation shuffle;
    shuffle.mutable_shuffle()->mutable_source()->MergeFrom(lastStripe);
    for (unsigned int i=0; i < columns.size(); i++) {
      shuffle.mutable_shuffle()->add_column(columns[i]);
      shuffle.mutable_shuffle()->add_type(types[columns[i]]);
    }
    for (unsigned int i = 0 ; i < keyColumns.size(); i++) {
      shuffle.mutable_shuffle()->add_hash_column(keyColumns[i]);
    }
    stripes.push_back(shuffle);
    
    groupBy.clear_source();
    query::UnionOperation* union_ = groupBy.mutable_source()->mutable_union_();
    for (unsigned i = 0; i < columns.size(); i++) {
      union_->add_column(columns[i]);
      union_->add_type(types[columns[i]]);
    }
    
    query::Operation groupByOp;
    groupByOp.mutable_group_by()->MergeFrom(groupBy);
    stripes.push_back(groupByOp);
  } else if (query.has_scan_file() || query.has_union_() || query.has_shuffle() || query.has_final()) {
    // this should never happen as those nodes are introduced by stripeOperation
    assert(false);
  }
  return stripes;
}

vector<query::ColumnType> getColumnTypes(const query::Operation& opProto) {
  Operation* op = Factory::createOperation(opProto);
  vector<query::ColumnType> result = op->getTypes();
  return result;
}

void addColumnsAndTypesToShuffle(query::ShuffleOperation& shuffle) {
  vector<query::ColumnType> types = getColumnTypes(shuffle.source());
  for (unsigned int i = 0; i < types.size(); i++) {
    shuffle.add_column(i);
    shuffle.add_type(types[i]);
  }
}

vector<query::Operation>* SchedulerNode::makeStripes(query::Operation query) {
  vector<query::Operation> stripes = stripeOperation(query);
  // add shuffle to the last but one stripe
  {
    query::Operation shuffleOp;
    query::Operation lastButOne = stripes.back();
    stripes.pop_back();
    shuffleOp.mutable_shuffle()->mutable_source()->MergeFrom(lastButOne);
    addColumnsAndTypesToShuffle(*shuffleOp.mutable_shuffle());
    stripes.push_back(shuffleOp);
  }
  // add the last stripe that is just union and final operation
  {
    query::Operation finalOp;
    query::UnionOperation& unionOp = *finalOp.mutable_final()->mutable_source()->mutable_union_();
    query::Operation& lastButOne = stripes.back();
    assert(lastButOne.has_shuffle());
    for (int i = 0; i < lastButOne.shuffle().column_size(); i++) {
      unionOp.add_column(lastButOne.shuffle().column(i));
      unionOp.add_type(lastButOne.shuffle().type(i));
    }
    stripes.push_back(finalOp);
  }
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
void assignNodesToUnion(query::Operation& stripe, const vector< std::pair<int, int> > & stripeIds) {
  if (stripe.has_scan()) {
    // stripes should never have plain scan
    assert(false);
  } else if (stripe.has_compute()) {
    assignNodesToUnion(*stripe.mutable_compute()->mutable_source(), stripeIds);
  } else if (stripe.has_filter()) {
    assignNodesToUnion(*stripe.mutable_filter()->mutable_source(), stripeIds);
  } else if (stripe.has_group_by()) {
    assignNodesToUnion(*stripe.mutable_group_by()->mutable_source(), stripeIds);
  } else if (stripe.has_scan_file()) {
    // only the first stripe can have scan own operation and this method
    // should not be called for the first stripe
    assert(false);
  } else if (stripe.has_shuffle()) {
    assignNodesToUnion(*stripe.mutable_shuffle()->mutable_source(), stripeIds);
  } else if (stripe.has_union_()) {
    for (unsigned int i = 0; i < stripeIds.size(); i++) {
      query::UnionOperation_Source* src = stripe.mutable_union_()->add_source();
      src->set_node(stripeIds[i].first);
      src->set_stripe(stripeIds[i].second);
    }
  } else if (stripe.has_final()) {
    assignNodesToUnion(*stripe.mutable_final()->mutable_source(), stripeIds);
  }
}

void assignReceiversCount(query::Operation& stripe, int receiversCount) {
  // the last operation (the root of stripe) should be shuffle
  assert(stripe.has_shuffle());
  stripe.mutable_shuffle()->set_receiverscount(receiversCount);
}

int extractInputFilesNumber(const query::Operation& query) {
  if (query.has_scan()) {
    return query.scan().number_of_files();
  } else if (query.has_compute()) {
    return extractInputFilesNumber(query.compute().source());
  } else if (query.has_filter()) {
    return extractInputFilesNumber(query.filter().source());
  } else if (query.has_group_by()) {
    return extractInputFilesNumber(query.group_by().source());
  } else if (query.has_scan_file()) {
    // if we have scan_file it means that the original scan operation has been
    // erased and there's no way to extract number of input files
    assert(false);
  } else if (query.has_shuffle()) {
    return extractInputFilesNumber(query.shuffle().source());
  } else if (query.has_union_()) {
    // if query has union as its source then it means we cannot extract number
    // of files (probably the wrong stripe has been passed)
    assert(false);
  } else if (query.has_final()) {
    return extractInputFilesNumber(query.final().source());
  } else {
    // unknown case
    assert(false);
  }
}

/*
 * Assigns file to ScanFileOperation. This method should be called with
 * the first stripe only.
 *
 * This method mutates passed stripe.
 */
void assignFileToScan(query::Operation& stripe, int fileToScan) {
  if (stripe.has_scan()) {
    // stripes should never have plain scan
    assert(false);
  } else if (stripe.has_compute()) {
    assignFileToScan(*stripe.mutable_compute()->mutable_source(), fileToScan);
  } else if (stripe.has_filter()) {
    assignFileToScan(*stripe.mutable_filter()->mutable_source(), fileToScan);
  } else if (stripe.has_group_by()) {
    assignFileToScan(*stripe.mutable_group_by()->mutable_source(), fileToScan);
  } else if (stripe.has_scan_file()) {
    stripe.mutable_scan_file()->set_source(fileToScan);
  } else if (stripe.has_shuffle()) {
    assignFileToScan(*stripe.mutable_shuffle()->mutable_source(), fileToScan);
  } else if (stripe.has_union_()) {
    // union is the source, probably the wrong stripe passed
    assert(false);
  } else if (stripe.has_final()) {
    assignFileToScan(*stripe.mutable_final()->mutable_source(), fileToScan);
  }
}

void SchedulerNode::schedule(vector<query::Operation> *stripes, uint32_t nodes, int numberOfFiles) {
  int nodesPerStripe = nodes / 2;
  // we don't handle situation when we have one worker only
  assert(nodesPerStripe > 0);
  // we divide workers into two groups that will be used for two consecutive layers of stripes
  vector<int> nodeIds;
  vector<int> nodeIdsNext;
  for (int i = 2; i <= nodesPerStripe; i++) {
    nodeIds.push_back(i);
  }
  for (uint32_t i = nodesPerStripe+1; i <= nodes; i++) {
    nodeIdsNext.push_back(i);
  }
  
  printf("nodeIds: ");
  for (unsigned int i = 0; i < nodeIds.size(); i++) {
    printf("%d, ", nodeIds[i]);
  }
  printf("\n");
  printf("nodeIdsNext: ");
  for (unsigned int i = 0; i < nodeIdsNext.size(); i++) {
    printf("%d, ", nodeIdsNext[i]);
  }
  printf("\n");
  
  vector< std::pair<int, int> > previousStripeIds;
  int stripeId = 0;
  // process the first stripe
  {
    query::Operation& firstStripe = stripes->front();
    // TODO: special hack for case of only two stripes, I need to refactor this
    // code to handle it in more elegant way
    if (stripes->size() == 2) {
      assignReceiversCount(firstStripe, 1);
    } else {
      assignReceiversCount(firstStripe, nodeIdsNext.size());
    }
    for (int i = 0; i < numberOfFiles; i++) {
      query::Operation stripeForFile = firstStripe; // make copy
      assignFileToScan(stripeForFile, i);
      int nodeId = nodeIds[i%nodeIds.size()];
      sendJob(stripeForFile, nodeId, stripeId);
      previousStripeIds.push_back(std::make_pair(nodeId, stripeId));
      stripeId++;
    }
  }
  std::swap(nodeIds, nodeIdsNext);
  // process inner stripes
  for (unsigned i = 1; i + 1 < stripes->size(); i++) {
    query::Operation& stripe = (*stripes)[i];
    vector< std::pair<int, int> > stripeIds;
    // assign to union nodes and stripes that were
    // sent in the previous iteration
    assignNodesToUnion(stripe, previousStripeIds);
    assignReceiversCount(stripe, nodeIdsNext.size());
    for (unsigned int j = 0; j < nodeIds.size(); j++) {
      int nodeId = nodeIds[j%nodeIds.size()];
      // make local copy of stripe because sendJob destroys its input data
      query::Operation localStripe = stripe;
      sendJob(localStripe, nodeId, stripeId);
      stripeIds.push_back(std::make_pair(nodeId, stripeId));
      stripeId++;
    }
    previousStripeIds = stripeIds;
    std::swap(nodeIds, nodeIdsNext);
  }
  // schedule the last stripe, use all the remaining workers
  {
    query::Operation lastStripe = stripes->back();
    assignNodesToUnion(lastStripe, previousStripeIds);
    sendJob(lastStripe, 1, stripeId);
  }
}

void SchedulerNode::sendJob(query::Operation &op, uint32_t node, int stripeId) {
  printf("Enqueing stripe[%d] to worker[%d]\n\n", stripeId, node);
  nodesJobs[node].push_back(std::make_pair(stripeId, op));
}

void SchedulerNode::flushJobs() {
  const google::protobuf::Reflection *r;
  string msg;
  
  for (uint32_t node = 0; node < nodesJobs.size(); node++) {
    if (nodesJobs[node].size() == 0)
      continue;
    
    query::Communication com;
    for (uint32_t i = 0; i < nodesJobs[node].size(); i++) {
      com.add_stripe();
      com.mutable_stripe(i)->set_stripe(nodesJobs[node][i].first);
      r = com.stripe(i).operation().GetReflection();
      r->Swap(&nodesJobs[node][i].second, com.mutable_stripe(i)->mutable_operation());
    }
    
    com.SerializeToString(&msg);
    printf("Sending jobs to worker[%d]\n\n%s", node, com.DebugString().c_str());
    nei->SendPacket(node, msg.c_str(), msg.size());
  }
}

void SchedulerNode::run(const query::Operation &op) {
  std::cout << "Scheduling proto: " << op.DebugString() << "\n";
  int numberOfInputFiles = extractInputFilesNumber(op);
  printf("number of input files: %d\n", numberOfInputFiles);
  vector<query::Operation> *stripes = makeStripes(op);
  std::cout << "after striping: " << std::endl;
  for (unsigned i=0; i < stripes->size() ; i++) {
    std::cout << "STRIPE " << i << std::endl;
    std::cout << (*stripes)[i].DebugString() << std::endl;
  }
  // node[0]: scheduler, node[1]: final operation
  schedule(stripes, nei->nodes_count() - 2, numberOfInputFiles);
  flushJobs();
  delete stripes;
  
  // TODO : switch to a worker mode
  // execPlan(finalOperation);
  return ;
}
