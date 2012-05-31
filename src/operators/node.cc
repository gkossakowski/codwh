// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#include "node.h"

std::ostream& operator<<(std::ostream& output, Node& node) {
  return node.debugPrint(output);
}

Node::~Node(){}
