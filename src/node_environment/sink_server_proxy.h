#ifndef SINK_SERVER_PROXY_H_
#define SINK_SERVER_PROXY_H_

#include "data_server.h"
#include "node_environment.h"

class SinkToServerProxy : public Server {
  DataSinkInterface* sink; // own it
 public:
  SinkToServerProxy(DataSinkInterface* sink_): sink(sink_) { }
  ~SinkToServerProxy() {
    delete sink;
  }

  // Not implemented Get* functions
  int GetDoubles(int column_index, int number, double* destination) {
    assert(false);
    return 0;
  }
  int GetInts(int column_index, int number, int32* destination) {
    assert(false);
    return 0;
  }
  int GetByteBools(int column_index, int number, bool* destination) {
    assert(false);
    return 0;
  }
  int GetBitBools(int column_index, int number, char* destination) {
    assert(false);
    return 0;
  }

  // consumes are just calling sink
  void ConsumeDoubles(int column_index, int number,
                              const double* destination) {
    return sink->ConsumeDoubles(column_index, number, destination);
  }
  void ConsumeInts(int column_index, int number,
                           const int32* destination) {
    return sink->ConsumeInts(column_index, number, destination);
  }
  void ConsumeByteBools(int column_index, int number,
                                const bool* destination) {
    return sink->ConsumeByteBools(column_index, number, destination);
  }
  void ConsumeBitBools(int column_index, int number,
                               const char* destination) {
    return sink->ConsumeBitBools(column_index, number, destination);
  }
};


#endif

