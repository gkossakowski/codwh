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

#include "operators/factory.h"
#include "operators/operation.h"
#include "node_environment/node_environment.h"
#include "distributed/node.h"

#include "proto/operations.pb.h"

int main(int argc, char** argv) {
  /** Read query */
  int queryFd = open(argv[1], O_RDONLY);
  FileInputStream queryFile(queryFd);
  query::Operation query;
  TextFormat::Parse(&queryFile, &query);
  queryFile.Close();

  /** Set up network environment */
  boost::scoped_ptr<NodeEnvironmentInterface> nei(
      CreateNodeEnvironment(argc - 2, argv + 2));

  /** Run job */
  SchedulerNode scheduler(nei.get(), query);
  scheduler.run();

  return 0;
}

