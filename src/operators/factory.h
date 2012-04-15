// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef FACTORY_H
#define FACTORY_H

#include "proto/operations.pb.h"
#include "server.h"

class Operation;
class ColumnProvider;

class Factory {
 public:
  static Server* server;
  static Operation* createOperation(const query::Operation& operation);
  static ColumnProvider* createColumnProvider(int columnId, int type);
};

#endif

