// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include <iostream>
#include <string>
#include <vector>
#include <boost/program_options.hpp>
#include <fstream>
#include <fcntl.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

using google::protobuf::TextFormat;
using google::protobuf::io::FileInputStream;

#include "operators/factory.h"
#include "operators/operation.h"

#include "proto/operations.pb.h"

namespace po = boost::program_options;
using std::string;
using std::vector;

int runQuery(const string server, int queryId, Operation* operation) {
  Factory::server = CreateServer(queryId, server);
  int remaining = 1;
  while (remaining > 0) {
    remaining = operation->consume();
  }

  return 0;
}

int main(int args, char** argv) {
  po::options_description desc("Allowed options");
  string server;
  desc.add_options()
      ("help", "produce help message")
      ("tree", "display query tree instead of running it")
      ("server", po::value<string>(&server)->default_value("default"), "choose server you want: test, default")
  ;

  po::options_description hidden("Hidden options");
  hidden.add_options()
      ("num", po::value<int>(), "num")
      ("queries", po::value<string>(), "queries")
  ;
  desc.add(hidden);

  po::positional_options_description p;
  p.add("num", 1).add("queries", 1);

  po::variables_map vm;
  po::store(po::command_line_parser(args, argv).
      options(desc).positional(p).run(), vm);
  po::notify(vm);

  if (vm.count("help") || !vm.count("queries") || !vm.count("num")) {
    std::cout << desc << "\n";
    return 1;
  }


  int queryNum = vm["num"].as<int>();
  string queryFilename = vm["queries"].as<string>();

  int queryFd = open(queryFilename.c_str(), O_RDONLY); 
  FileInputStream queryFile(queryFd);
  query::Operation rootOperation;
  TextFormat::Parse(&queryFile, &rootOperation);
  queryFile.Close();

  Operation* operation = Factory::createOperation(rootOperation);

  if (vm.count("tree")) {
    std::cout << "Query num " << queryNum;
    std::cout << " filename " << queryFilename << "\n";
    std::cout << "Displaying query tree output:\n";

    operation->debugPrint(std::cout);
    std::cout << std::endl;
    std::cout << rootOperation.DebugString() << std::endl;
  } 

  int ret = runQuery(server, queryNum, operation);

  delete Factory::server;

  return ret;
}

