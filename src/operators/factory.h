// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef FACTORY_H
#define FACTORY_H

#include "operation.h"
namespace query {
#include "proto/operations.pb.h"
}

class Factory {
 public:
  static Operation* createOperation(const query::Operation& operation);
};

#endif

