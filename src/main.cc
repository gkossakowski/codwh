// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include <iostream>
#include <string>
#include <vector>
#include <boost/program_options.hpp>

#include "operators/constant_operator.h"

namespace po = boost::program_options;
using std::string;
using std::vector;

int main(int args, char** argv) {
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "produce help message")
      ("tree", "display query tree instead of running it")
  ;

  po::options_description hidden("Hidden options");
  hidden.add_options()
      ("queries", po::value< vector<string> >(), "queries")
  ;
  desc.add(hidden);

  po::positional_options_description p;
  p.add("queries", -1);

  po::variables_map vm;
  po::store(po::command_line_parser(args, argv).
      options(desc).positional(p).run(), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 1;
  }

  if (vm.count("tree")) {
    std::cout << "Displaying query tree output:\n";

    ConstantOperator op;
    op.debugPrint(std::cout);
    std::cout << std::endl;
  } else {
  }
  return 0;
}

