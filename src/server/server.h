// Copyright 2012 Google Inc. All Rights Reserved.
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include <stdint.h>

typedef int32_t int32;

class Server {
 public:
  // In the case of the timed program, creating the server starts the watch, and
  // destroying the server stops the watch and triggers correctness checks
  // (obviously should be done once all the data was consumed).
  Server() {}
  virtual ~Server() {}

  // Fills |destination| with at most |number| entries, which are the subsequent
  // entries from column |column_index|. Returns the number of entries filled
  // out. A return value of zero should be interpreted as End Of Stream.
  virtual int GetDoubles(int column_index, int number, double* destination) = 0;
  virtual int GetInts(int column_index, int number, int32* destination) = 0;
  // Bools in the standard C++ format.
  virtual int GetByteBools(int column_index, int number, bool* destination) = 0;
  // Bools encoded as bitmasks.
  virtual int  GetBitBools(int column_index, int number, char* destination) = 0;

  // Consumes the results of the calculation. The first |number| entries in
  // |destination| are assumed to be the subsequent entries in column
  // |column_index|.
  virtual void ConsumeDoubles(int column_index, int number,
                              const double* destination) = 0;
  virtual void ConsumeInts(int column_index, int number,
                           const int32* destination) = 0;
  virtual void ConsumeByteBools(int column_index, int number,
                                const bool* destination) = 0;
  virtual void ConsumeBitBools(int column_index, int number,
                               const char* destination) = 0;
};

// The query id is the id that comes from the operations proto.
Server *CreateServer(int query_id);
