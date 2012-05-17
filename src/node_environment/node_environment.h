// Copyright 2012 Google Inc. All Rights Reserved.
// Author: onufry@google.com (Onufry Wojtaszczyk)
//         ptab@google.com (Piotr Tabor)
#ifndef NODE_ENVIRONMENT_H_
#define NODE_ENVIRONMENT_H_

#include <stdint.h>
#include <stdlib.h>
#include <string>

typedef int32_t int32;
typedef uint32_t uint32;

// DataSourceInterface and DataSinkInterface are the equivalent of the Server
// interface from the previous assignment.  return result;

// DataSourceInterface represents an abstraction of a single source file with
// data that need to be processed by the database.
// The DataSourceInterface implementation is not guaranteed to be thread-safe.
class DataSourceInterface {
 public:
  // Fills |destination| with at most |number| entries, which are the subsequent
  // entries from column |column_index|. Returns the number of entries filled
  // out. A return value of zero should be interpreted as End Of Stream.
  // Unpredictable behaviour if trying to read other data type than what was
  // declared by the source in the plan.
  virtual int GetDoubles(int column_index, int number, double* destination) = 0;
  virtual int GetInts(int column_index, int number, int32* destination) = 0;
  // Bools in the standard C++ format.
  virtual int GetByteBools(int column_index, int number, bool* destination) = 0;
  // Bools encoded as bitmasks.
  virtual int GetBitBools(int column_index, int number, char* destination) = 0;

  virtual ~DataSourceInterface() {};
};

// DataSinkInterface represents an abstraction of a single result file produced
// by the database. The produced data will be checked for correctness.
// Please note that the query is considered to be finished (the time stops) when
// all the nodes exited with status = 0. 
// The sink implementation is not guaranted to be thread-safe.
class DataSinkInterface {
 public:
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

  virtual ~DataSinkInterface() {};
};

// NodeEnvironmentInterface implementation is thread-safe.
class NodeEnvironmentInterface {
 public:
  // Returns node number of the process.
  virtual uint32 my_node_number() const = 0;

  // Returns the total number of nodes.
  virtual uint32 nodes_count() const = 0;

  // -------------- Inter-worker communication ---------------------------------

  // Sends a packet to given |target_node|. The packet
  // will be returned on the |target_node| from one
  // of the subsequent ReadPacket* calls. Note that
  // this call can block, if network buffers are full
  // (e.g. consumer is not reading packets).
  // There will be no interleaving between packets and two packets 
  // will not arrive as a single packet.
  virtual void SendPacket(uint32 target_node,
                          const char* data,
                          int  data_len) = 0;

  // Reads a single packet sent to this node.
  // Blocks if there is not packet ready until the packet arrive.
  // Caller takes ownership of the returned packet and should
  // destroy it using delete[].
  virtual char* ReadPacketBlocking(std::size_t* data_len) = 0;

  // Returns NULL if there is no packet waiting
  // Updates data_len to contain the size of the read packet.
  // Caller takes ownership of the returned packet and should
  // destroy it using delete[].
  virtual char* ReadPacketNotBlocking(std::size_t* data_len) = 0;

  // -------- Reading input files and writing results --------------------------

  // Caller takes ownership of the source.
  virtual DataSourceInterface* OpenDataSourceFile(int source_file_id) = 0;

  // Caller takes ownership of the sink.
  // You should open a single sink only once query-wide 
  // (unpredictable behavior otherwise).
  virtual DataSinkInterface* OpenDataSink() = 0;

  virtual ~NodeEnvironmentInterface() {};
};

// Assumes that parameters meaning is as follows:
// ./program_name [listening_port] target_node_1_host:port target_node_2_host:port .. target_node_n_host:port
NodeEnvironmentInterface* CreateNodeEnvironment(int argc, char** argv);

#endif  // NODE_ENVIRONMENT_H_

