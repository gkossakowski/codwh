// Copyright 2012 Google Inc. All Rights Reserved.
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include <cassert>
#include <cstdio>
#include "server.h"

int main() {
  int tab[5];
  double dtab[5];
  bool btab[8];
  char ctab[8];
  class Server *server = CreateServer(0);

  assert(server->GetInts(0, 4, tab) == 4);
  server->ConsumeInts(0, 4, tab);
  assert(server->GetInts(0, 4, tab) == 4);
  server->ConsumeInts(0, 4, tab);
  assert(server->GetInts(0, 4, tab) == 2);
  server->ConsumeInts(0, 2, tab);
  assert(server->GetInts(1, 1, tab) == 1);
  assert(server->GetInts(0, 1, tab) == 0);

  assert(server->GetDoubles(2, 3, dtab) == 3);
  assert(server->GetByteBools(3, 6, btab) == 6);
  assert(server->GetBitBools(3, 6, ctab) == 4);
  return 0;
}
