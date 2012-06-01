#include "packet.h"

NodePacket::NodePacket(vector<Column*> &view)
  : size(0), readyToSend(false)
{
  size_t row_size = 0;
  columns.resize(view.size(), NULL);
  offsets.resize(view.size(), 0);
  types.resize(view.size(), query::INVALID_TYPE);

  // collect row size and types
  for (uint32_t i = 0; i < view.size(); i++) {
    types[i] = view[i]->getType();
    row_size += global::getTypeSize(types[i]);
  }

  if (view.empty()) {
    return; // it is eof packet
  }

  // compute maximum capacity
  capacity = MAX_PACKET_SIZE / row_size;

  // allocate space and reset data counters;
  for (uint32_t i = 0; i < view.size(); i++)
    columns[i] = new char[capacity * global::getTypeSize(types[i])];
}

/** Tries to consume a view. If it's too much data - returns false. */
void NodePacket::consume(vector<Column*> view){
  assert(!readyToSend); // should check if it's full before calling!
  assert(!isEOF());

  for (uint32_t i = 0; i < view.size(); i++)
    offsets[i] += view[i]->transfuse(columns[i], offsets[i]);
  size += static_cast<size_t>(view[0]->size);

  if (capacity - size < static_cast<size_t>(DEFAULT_CHUNK_SIZE))
    readyToSend = true;
}

query::DataPacket* NodePacket::serialize() {
  query::DataPacket *packet = new query::DataPacket();

  for (uint32_t i = 0; i < types.size(); i++) {
    packet->add_type(types[i]);
    packet->add_data(columns[i], offsets[i]);
  }
  return packet;
}

NodePacket::~NodePacket() {
  for(uint32_t i = 0; i < columns.size(); i++)
    delete[] columns[i];
}
