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

#include <execinfo.h>
#include <signal.h>

void handler(int sig) {
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, 2);
  exit(1);
}


int main(int argc, char** argv) {
  signal(SIGSEGV, handler);
  /** Set up network environment */
  boost::scoped_ptr<NodeEnvironmentInterface> nei(
      CreateNodeEnvironment(argc, argv));

  /** Run job */
  WorkerNode worker(nei.get());
  worker.run();

  return 0;
}

