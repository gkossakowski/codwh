// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef NODE_H
#define NODE_H

#include <fstream>

/** Base class for all nodes in query */
class Node {
 public:
  /** Prints the tree.*/
  virtual std::ostream& debugPrint(std::ostream& output) = 0; 
};

std::ostream& operator<<(std::ostream& output, Node& node);

#endif // NODE_H

