#include "node_environment.h"

#include <boost/scoped_ptr.hpp>
#include <string.h>
#include <vector>

#include <stdlib.h>
#include <stdio.h>

#include "utils/ip_address.h"
#include "utils/logger.h"
#include "netio/network_input.h"
#include "netio/network_output.h"
#include "node_environment/data_server.h"

namespace {

class DataSource : public DataSourceInterface {
 public:
  DataSource(Server* server) : data_server_(server) {}

  virtual int GetDoubles(int column_index, int number, double* destination) {
    return data_server_->GetDoubles(column_index, number, destination);
  }

  virtual int GetInts(int column_index, int number, int32* destination) {
    return data_server_->GetInts(column_index, number, destination);
  }

  virtual int GetByteBools(int column_index, int number, bool* destination) {
    return data_server_->GetByteBools(column_index, number, destination);
  }

  virtual int GetBitBools(int column_index, int number, char* destination) {
    return data_server_->GetBitBools(column_index, number, destination);
  }

 private:
  boost::scoped_ptr<Server> data_server_;
};

// ----------------------------------------------------------------------------

class DataSink : public DataSinkInterface {
 public:
  DataSink(Server* server) : data_server_(server) {}

  virtual void ConsumeDoubles(int column_index, int number,
                              const double* destination) {
    data_server_->ConsumeDoubles(column_index, number, destination);
  }

  virtual void ConsumeInts(int column_index, int number,
                           const int32* destination) {
    data_server_->ConsumeInts(column_index, number, destination);
  }

  virtual void ConsumeByteBools(int column_index, int number,
                                const bool* destination) {
    data_server_->ConsumeByteBools(column_index, number, destination);
  }

  virtual void ConsumeBitBools(int column_index, int number,
                               const char* destination) {
    data_server_->ConsumeBitBools(column_index, number, destination);
  }

 private:
  boost::scoped_ptr<Server> data_server_;
};

// ----------------------------------------------------------------------------

class NodeEnvironment : public NodeEnvironmentInterface {
 public:
  NodeEnvironment(uint32 node_number,
                  int query_num,
                  NetworkInput* input,
                  std::vector<NetworkOutput*> outputs)
  : node_number_(node_number),
    node_count_(outputs.size()),
    input_(input),
    outputs_(outputs),
    kQueryId(query_num) {}

  virtual uint32 my_node_number() const {
    CHECK(node_number_>=0, "");
    return node_number_;
  }

  virtual uint32 nodes_count() const {
    CHECK(node_count_>=0, "");
    return node_count_;
  }

  virtual void SendPacket(uint32 target_node,
                          const char* data,
                          int  data_len) {
    CHECK(target_node < nodes_count(), "");
    CHECK(target_node >= 0, "");
    CHECK(outputs_[target_node]->SendPacket(data, data_len), "");
  }

  virtual char* ReadPacketBlocking(std::size_t* data_len) {
    return input_->ReadPacketBlocking(data_len);
  }

  virtual char* ReadPacketNotBlocking(std::size_t* data_len) {
    return input_->ReadPacketNotBlocking(data_len);
  }

  // -------- Reading input files and writing results --------------------------

  virtual DataSourceInterface* OpenDataSourceFile(int source_file_id) {
    return new DataSource(CreateServer(kQueryId));
  };

  virtual DataSinkInterface* OpenDataSink() {
    return new DataSink(CreateServer(kQueryId));
  };

 protected:
  virtual ~NodeEnvironment() {
    for (std::vector<NetworkOutput*>::const_iterator it = outputs_.begin();
         it != outputs_.end(); ++it) {
      delete (*it);
    }
    outputs_.clear();
  }

 private:
  int kQueryId;
  uint32 node_number_;
  uint32 node_count_;
  boost::scoped_ptr<NetworkInput> input_;
  std::vector<NetworkOutput*> outputs_;
};

} // namespace

NodeEnvironmentInterface* CreateNodeEnvironment(int argc, char** argv) {
  CHECK(argc >= 5, "Requires at least 4 arguments [node number] [port] [query num] [target_node:port]*");
  int node_number = atoi(argv[1]);
  int listening_port = atoi(argv[2]);
  int query_num = atoi(argv[3]);
  std::vector<NetworkOutput*> outputs;
  for (int i = 4; i < argc; ++i) {
    IpAddress address = IpAddress::Parse(argv[i]);
    outputs.push_back(new NetworkOutput(address.getHostAddress(),
                                        address.getService()));
  }
  LOG1("Running server listening on port: %d", listening_port);
  return new NodeEnvironment(node_number, query_num, new NetworkInput(listening_port), outputs);
}
