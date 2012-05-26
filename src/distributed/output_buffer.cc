#include "output_buffer.h"
#include "communication.h"

void OutputBuffer::resetOutput(int buckets) {
  for (uint32_t i = 0; i < output.size(); i++)
    if (output[i].size() != 0) assert(false); // calling during processing is forbidden

  output.resize(0); // drop current objects (just in case)
  output.resize(buckets);
  pending_requests.resize(0);
  pending_requests.resize(buckets, 0);
  output_counters.resize(0);
  output_counters.resize(buckets, 0);
  consumers_map.resize(0);
  consumers_map.resize(buckets, -1);
  full_packets = 0;
}

void OutputBuffer::parseRequests() {
  communication->debugPrint("parseRequests...");
  int bucket;
  int provider_stripe;
  query::DataRequest *request;

  communication->debugPrint("requests.size = %lu", communication->requests.size());
  while (communication->requests.size() > 0) {
    request = communication->requests.front();
    communication->requests.pop();
    provider_stripe = request->provider_stripe();
    if (provider_stripe != *communication->stripe) {
      // future stripe, save for later
      assert(provider_stripe > *communication->stripe);
      communication->debugPrint("[QUEUE] Add request for strip %d while processing stripe %d",
          provider_stripe, *communication->stripe);
      typeof(communication->delayed_requests.begin()) it =
             communication->delayed_requests.find(provider_stripe);
      if (it == communication->delayed_requests.end()) {
        queue<query::DataRequest*> q;
        q.push(request);
        communication->delayed_requests.insert(make_pair(provider_stripe, q));
      } else {
        it->second.push(request);
      }
      continue;
    }

    bucket = request->consumer_stripe() % output.size();
    communication->debugPrint("request from stripe %d, pending for bucket %d",
        request->consumer_stripe(), bucket);
    consumers_map[bucket] = request->node();
    pending_requests[bucket]++;

    flushBucket(bucket); // try to send data
    delete request;
  }
}

void OutputBuffer::packData(vector<Column*> &data, int bucket) {
  queue<NodePacket*> &buck = output[bucket];

  if (buck.size() == 0 || buck.back()->readyToSend)
    buck.push(new NodePacket(data));
  buck.back()->consume(data);

  if (buck.back()->readyToSend)
    full_packets++;
  assert(full_packets <= MAX_OUTPUT_PACKETS);

  // read all incoming requests
  query::NetworkMessage *message;
  while ((message = communication->getMessage(false)) != NULL) {
    communication->parseMessage(message, false); // we should have all data already
  }

  bool flag;
  do {
    parseRequests();
    flag = (full_packets == MAX_OUTPUT_PACKETS);
    if (flag)
      communication->getRequest();
  } while (flag);

  // finally, try to flush current bucket
  flushBucket(bucket);
}

void OutputBuffer::flushBucket(int bucket) {
  communication->debugPrint("flushBucket(%d)", bucket);
  if (pending_requests[bucket] == 0) {
    communication->debugPrint("flushBucket: no pending request for bucket");
    return;
  }
  assert(!output[bucket].empty());
  if (!output[bucket].front()->readyToSend) {
    communication->debugPrint("flushBucket: packet not ready to send for bucket");
    return;
  }
  assert(consumers_map[bucket] != -1); // we should know node number from request

  query::NetworkMessage com;
  query::DataResponse *response = com.mutable_data_response();
  query::DataPacket *packet;
  const google::protobuf::Reflection *r = response->data().GetReflection();
  string msg;

  response->set_node(communication->nei->my_node_number());
  // send data while we have a full packet and a pending request
  while (pending_requests[bucket] > 0 && output[bucket].front()->readyToSend) {
    packet = output[bucket].front()->serialize();
    output_counters[bucket]++; // increase packet number counter

    response->set_number(output_counters[bucket]); // set number
    r->Swap(response->mutable_data(), packet); // set data
    com.SerializeToString(&msg);
    communication->debugPrint("[SEND] sending data response to %d", consumers_map[bucket]);
    communication->nei->SendPacket(consumers_map[bucket], msg.c_str(), msg.size()); // send

    output[bucket].pop(); // remove packet from queue
    pending_requests[bucket]--;
    full_packets--;
    delete packet; // dump packet
  }
}

void OutputBuffer::sendEof() {
  query::NetworkMessage com;
  query::DataResponse *response = com.mutable_data_response();
  string msg;

  communication->debugPrint("sendEof");

  response->set_node(communication->nei->my_node_number());
  response->set_number(-1); // EOF response
  com.SerializeToString(&msg); // universal message for all consumers

  for (uint32_t i = 0; i < output.size(); i++) {
    while (consumers_map[i] == -1) { // unknown consumer!
      communication->getRequest();
      parseRequests(); // updates the consumer_map while parsing a request
    }
    communication->nei->SendPacket(consumers_map[i], msg.c_str(), msg.size());
  }
}

