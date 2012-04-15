// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "factory.h"

Operation*
Factory::createOperation(const query::Operation& operation) {
  if (operation.has_scan()) {
    return new ScanOperation(operation.scan());
  } else {
    assert(false);
    return NULL;
  }
}

