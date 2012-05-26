#include "input_buffer.h"
#include "communication.h"

void InputBuffer::sendRequest(int provider_stripe, int number, int node) {
  string msg;
  query::NetworkMessage com;
  com.data_request();

  query::DataRequest *request = com.mutable_data_request();
  request->set_node(communication->nei->my_node_number());
  request->set_provider_stripe(provider_stripe);
  request->set_consumer_stripe(*communication->stripe);
  request->set_number(number);

  com.SerializeToString(&msg);
  communication->debugPrint("[SEND] Sending data request to %d msg={%s}", node,
      com.DebugString().c_str());
  communication->nei->SendPacket(node, msg.c_str(), msg.size());
}
