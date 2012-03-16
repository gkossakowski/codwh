// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef OPERATOR_H
#define OPERATOR_H

#include <iostream>
#include <fstream>


class Operator {
 public:
  /**
   * Prints the operator tree.
   */
  virtual std::ostream& debugPrint(std::ostream& output) = 0; 
};

#endif // OPERATOR_H
