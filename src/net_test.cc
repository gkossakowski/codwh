#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>

#include "node_environment/node_environment.h"
#include "utils/logger.h"

void Writer(NodeEnvironmentInterface* nei) {
  for (int i = 0 ; i < 100; ++i) {
    for (int j = 0; j < nei->nodes_count(); ++j) {
      nei->SendPacket(j, "HEYHEY", 7);
    }
  }
}

void Reader(NodeEnvironmentInterface* nei) {
  for (int i = 0 ; i < 5 * 100 * nei->nodes_count(); ++i) {
    std::size_t data_len;
    boost::scoped_array<char> packet(nei->ReadPacketBlocking(&data_len));
    if (packet != NULL) {
      LOG3("worker %d/%d: GOT: %s", nei->my_node_number(), nei->nodes_count(), packet.get());
    }
  }
  std::size_t data_len;
  boost::scoped_array<char> packet(nei->ReadPacketNotBlocking(&data_len));
  CHECK(packet == NULL, "");
}


int main(int argc, char** argv) {
  boost::scoped_ptr<NodeEnvironmentInterface> nei(
      CreateNodeEnvironment(argc, argv));

  // Lets open data sources.
  boost::scoped_ptr<DataSourceInterface> data_source_0(nei->OpenDataSourceFile(0));
  boost::scoped_ptr<DataSourceInterface> data_source_1(nei->OpenDataSourceFile(1));
  // Lets open sink on node 0.
  if (nei->my_node_number() == 0) {
    boost::scoped_ptr<DataSinkInterface> data_sink(nei->OpenDataSink());
  }
  if (nei->my_node_number() == nei->nodes_count() - 1) {
    // Last node sends INIT to the 0 node and waits for INIT from nei->nodes_count() - 2 node.
    nei->SendPacket(0, "INIT", 5);
    std::size_t data_len;
    boost::scoped_array<char> packet(nei->ReadPacketBlocking(&data_len));
    CHECK(data_len == 5, "");
    CHECK(strcmp("INIT", packet.get()) == 0, "");
  } else {
    std::size_t data_len;
    boost::scoped_array<char> packet(nei->ReadPacketBlocking(&data_len));
    CHECK(data_len == 5, "");
    CHECK(strcmp("INIT", packet.get()) == 0, "");
    nei->SendPacket(nei->my_node_number() + 1, "INIT", 5);
  }
  sleep(5);
  boost::thread t0(&Reader, nei.get());
  boost::thread t1(&Writer, nei.get());
  boost::thread t2(&Writer, nei.get());
  boost::thread t3(&Writer, nei.get());
  boost::thread t4(&Writer, nei.get());
  boost::thread t5(&Writer, nei.get());
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  t5.join();
  t0.join();
  return 0;
}

