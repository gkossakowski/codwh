// Copyright 2012 Google Inc. All Rights Reserved.
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include <cassert>
#include "server.h"

#define TABLESIZE 16

int main() {
  class Server *server = CreateServer(1);

  int tab[TABLESIZE + 1];
  double dtab[TABLESIZE + 1];
  bool btab[TABLESIZE + 1];
  char ctab[TABLESIZE + 1];

  tab[TABLESIZE] = 19;
  dtab[TABLESIZE] = 19;
  btab[TABLESIZE] = true;
  ctab[TABLESIZE / 8] = 11;

  server->GetInts(0, TABLESIZE, tab);
  server->ConsumeInts(0, TABLESIZE, tab);

  server->GetInts(1, TABLESIZE, tab);
  server->ConsumeInts(1, TABLESIZE, tab);

  server->GetDoubles(2, TABLESIZE, dtab);
  server->ConsumeDoubles(2, TABLESIZE, dtab);

  server->GetByteBools(3, TABLESIZE, btab);
  server->ConsumeByteBools(3, TABLESIZE, btab);

  server->GetBitBools(3, TABLESIZE, ctab);
  server->ConsumeBitBools(3, TABLESIZE, ctab);

  assert(tab[TABLESIZE] == 19);
  assert(dtab[TABLESIZE] == 19);
  assert(btab[TABLESIZE]);
  assert(ctab[TABLESIZE / 8] == 11);
  return 0;
}
