#include "distributed/node.h"
#include "operators/column.h"

#include <vector>

using std::vector;


void test_serialize() {
  vector<Column*> vec;
  ColumnChunk<double> double_col;
  ColumnChunk<int> int_col;
  ColumnChunk<char> bool_col;
  vector<bool> col_mask;
  Packet *packet;

  for (int i = 0; i < DEFAULT_CHUNK_SIZE; i++) {
    double_col.chunk[i] = i * 5.0;
    int_col.chunk[i] = i;
    bool_col.chunk[i] = static_cast<char>(i % 256);
  }
  double_col.size = DEFAULT_CHUNK_SIZE;
  int_col.size = DEFAULT_CHUNK_SIZE;
  bool_col.size = DEFAULT_CHUNK_SIZE;

  vec.push_back(&double_col);
  col_mask.push_back(true);
  vec.push_back(&int_col);
  col_mask.push_back(false);
  vec.push_back(&bool_col);
  col_mask.push_back(true);
  vec.push_back(&double_col);
  col_mask.push_back(false);
  vec.push_back(&int_col);
  col_mask.push_back(true);
  vec.push_back(&bool_col);
  col_mask.push_back(true);

  packet = new Packet(vec, col_mask);
  packet->consume(vec);
  std::cout << packet->serialize()->DebugString() << std::endl;

  return ;
}


int main() {
  test_serialize();

  return 0;
}
