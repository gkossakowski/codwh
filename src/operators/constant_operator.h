// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef OPERATOR_H
#define OPERATOR_H

#include <iostream>

#include "node.h"
#include "operator.h"

class ConstantOperator : public Node {
 public:
  std::ostream& debugPrint(std::ostream& output);  
};

#endif
