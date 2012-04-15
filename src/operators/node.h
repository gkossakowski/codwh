// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef NODE_H
#define NODE_H

#include <iostream>
#include <fstream>


class Node {
 public:
  /**
   * Prints the tree.
   */
  virtual std::ostream& debugPrint(std::ostream& output) = 0; 
};

#endif // NODE_H

