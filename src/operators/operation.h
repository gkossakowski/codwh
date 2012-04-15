// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef OPERATION_H
#define OPERATION_H

#include <iostream>
#include <fstream>
#include <vector>
#include "node.h"
#include "column.h"
#include "factory.h"

#include "proto/operations.pb.h"

using std::vector;

class Operation : public Node {
 protected:
  vector<Column*> cache;
 public:
  virtual vector<Column*>* pull() = 0;

  int consume() {
    vector<Column*>* ptr = pull();

    if (ptr->empty()) {
      return 0;
    }

    for (unsigned i = 0 ; i < ptr->size() ; ++i) {
      (*ptr)[i]->consume(i, Factory::server);
    }

    return (*ptr)[0]->size;
  }

};

class ScanOperation : public Operation {
  vector<ColumnProvider*> providers;
 public:
  ScanOperation(const query::ScanOperation& oper) {
    int n = oper.column_size();
    cache = vector<Column*>(n);
    providers = vector<ColumnProvider*>(n);

    for (int i = 0 ; i < n ; ++i) {
      providers[i] = Factory::createColumnProvider(
          oper.column().Get(i), oper.type().Get(i));
    }
  }

  vector<Column*>* pull() {
    for (unsigned i = 0 ; i < providers.size() ; ++i) {
      cache[i] = providers[i]->pull();
    }
    return &cache;
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << "ScanOperation {";
    for (unsigned i = 0 ; i < providers.size() ; ++i) {
      providers[i]->debugPrint(output);
      output << ", ";
    }
    return output << "}\n";
  }

  ~ScanOperation() {
    // TODO: free providers
  }
};

class ComputeOperation : public Operation {
  Operation* source;
  // TODO: expressions
 public:
  ComputeOperation(const query::ComputeOperation& oper) {
    source = Factory::createOperation(oper.source());
  }

  vector<Column*>* pull() {
    return source->pull();
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << "ComputeOperation { \nsource = ";
    source->debugPrint(output);
    return output << "}\n";
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

