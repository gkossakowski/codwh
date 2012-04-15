// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef OPERATION_H
#define OPERATION_H

#include <iostream>
#include <fstream>
#include "node.h"

#include "proto/operations.pb.h"

class Operation : public Node {
 public:
};

class ScanOperation : public Operation {
 public:
  ScanOperation(const query::ScanOperation& oper) {
  }
  std::ostream& debugPrint(std::ostream& output) {
    return output << "ScanOperation";
  }
};

class ComputeOperation : public Operation {
 public:
  std::ostream& debugPrint(std::ostream& output) {
    return output << "ComputeOperation";
  }
};

class FilterOperation : public Operation {
 public:
  std::ostream& debugPrint(std::ostream& output) {
    return output << "FilterOperation";
  }
};

class GroupByOperation : public Operation {
 public:
  std::ostream& debugPrint(std::ostream& output) {
    return output << "GroupByOperation";
  }
};

#endif // OPERATION_H

