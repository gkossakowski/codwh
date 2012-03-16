// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include <iostream>

#include "operators/constant_operator.h"

int main() {
  ConstantOperator op;
  op.debugPrint(std::cout);
  std::cout << std::endl;
  return 0;
}

