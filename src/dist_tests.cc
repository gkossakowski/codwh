#include "distributed/node.h"
#include "operators/column.h"

#include <vector>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

using google::protobuf::TextFormat;
using google::protobuf::io::FileInputStream;

#include <boost/scoped_ptr.hpp>
using std::vector;


vector<Column*>* test_vector() {
  vector<Column*> *vec = new vector<Column*>();
  ColumnChunk<double> *double_col = new ColumnChunk<double>();
  ColumnChunk<int> *int_col = new ColumnChunk<int>();
  ColumnChunk<char> *bool_col = new ColumnChunk<char>();

  for (int i = 0; i < DEFAULT_CHUNK_SIZE; i++) {
    double_col->chunk[i] = i * 5.0;
    int_col->chunk[i] = i;
    bool_col->chunk[i] = static_cast<char>(i % 256);
  }
  double_col->size = DEFAULT_CHUNK_SIZE;
  int_col->size = DEFAULT_CHUNK_SIZE;
  bool_col->size = DEFAULT_CHUNK_SIZE;

  vec->push_back(double_col);
  vec->push_back(int_col);
  vec->push_back(bool_col);
  vec->push_back(double_col);
  vec->push_back(int_col);
  vec->push_back(bool_col);

  return vec;
}

void drop_test_vector(vector<Column*> *vec) {
  for (uint32_t i = 0; i < vec->size() / 2; i++)
    delete (*vec)[i];
  delete vec;
}

vector<bool>* test_col_mask() {
  vector<bool> *mask = new vector<bool>();
  mask->push_back(true);
  mask->push_back(false);
  mask->push_back(true);
  mask->push_back(false);
  mask->push_back(true);
  mask->push_back(true);

  return mask;
}

void test_serialize() {
  /* should print out a serialized, full, package */

  vector<bool> *col_mask = test_col_mask();
  vector<Column*> *vec = test_vector();
  Packet *packet;

  packet = new Packet(*vec, *col_mask);
  while (!packet->full) {
    packet->consume(*vec);
  }
  std::cout << packet->serialize()->DebugString() << std::endl;

  drop_test_vector(vec);
  delete col_mask;
}

void test_packaging() {
  /* should pack data as long as there's space in a buffer */
  int argc = 4;
  char *argv[] = {"worker", "0", "2000", "localhost:2000" };
  vector<bool> *col_mask = test_col_mask();
  vector<Column*> *vec = test_vector();

  boost::scoped_ptr<NodeEnvironmentInterface> nei(
      CreateNodeEnvironment(argc, argv));

  WorkerNode worker(nei.get());
  worker.resetOutput(4, *col_mask);
  for( int i = 0; i < 10000; i++) {
    std::cout << "writing chunk nr " << i << " into bucket" << i % 4 << std::endl;
    worker.packData(*vec, i % 4);
  }
}


int main() {
  //test_serialize();
  test_packaging();

  return 0;
}
