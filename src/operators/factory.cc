// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "factory.h"

#include "operation.h"
#include "column.h"

Server* Factory::server;

Operation*
Factory::createOperation(const query::Operation& operation) {
  std::cout << "ABC" << operation.DebugString();
  if (operation.has_scan()) {
    return new ScanOperation(operation.scan());
  } else if (operation.has_compute()) {
    return new ComputeOperation(operation.compute());
  } else {
    assert(false);
    return NULL;
  }
}

ColumnProvider*
Factory::createColumnProvider(int columnId, int type) {
  switch (type) {
    case query::ScanOperation_Type_INT:
      return new ColumnProviderImpl<int>(columnId);
    case query::ScanOperation_Type_DOUBLE:
      return new ColumnProviderImpl<double>(columnId);
    case query::ScanOperation_Type_BOOL:
      return new ColumnProviderImpl<char>(columnId);
    default:
      assert(false);
  }
}
