#include <iostream>
#include <string>
#include <vector>
#include <boost/program_options.hpp>
#include <fstream>
#include <fcntl.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <boost/scoped_ptr.hpp>

using google::protobuf::TextFormat;
using google::protobuf::io::FileInputStream;

#include "node_environment/data_server.h"
#include "operators/factory.h"
#include "operators/operation.h"
#include "node_environment/node_environment.h"
#include "distributed/node.h"

#include "proto/operations.pb.h"

int main(int argc, char** argv) {
  /** Set up network environment */
  boost::scoped_ptr<NodeEnvironmentInterface> nei(
      CreateNodeEnvironment(argc, argv));

  // Ugly, temporary hack
  printf("WARNING: New server, query id = %d\n", atoi(argv[argc - 1]));
  Factory::server = CreateServer(atoi(argv[argc - 1]));

  /** Run job */
  WorkerNode worker(nei.get());
  worker.run();

  return 0;
}

