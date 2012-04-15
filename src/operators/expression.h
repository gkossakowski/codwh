// Fast And Furious column database
// Author: Jacek Migdal <jacek@migdal.pl>

#ifndef EXPRESSION_H
#define EXPRESSION_H

#include <vector>
#include <string>
#include <algorithm>

#include "column.h"

using std::swap;
using std::vector;
using std::string;

class Expression : public Node {
 public:
  virtual Column* pull(vector<Column*>* sources) = 0;
  virtual int getType() = 0;
};

template<class T>
class Expression2 : public Expression {
 protected:
  ColumnChunk<T> cache;
  Expression* left;
  Expression* right;
  virtual void pullInt(Column* leftData, Column* rightData) = 0;
  virtual string getName() { return ""; }
  virtual string getSymbol() { return ""; }

 public:
  Expression2(Expression* l, Expression* r): left(l), right(r) { }

  Column* pull(vector<Column*>* sources) {
    Column* leftData = left->pull(sources);
    Column* rightData = right->pull(sources);
    pullInt(leftData, rightData);
    return &cache;
  }

  int getType() {
    return global::getType<T>();
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << getName() << "(";
    left->debugPrint(output);
    output << " " << getSymbol() << " ";
    right->debugPrint(output);
    return output << ")";
  }
};

template<class T>
class ExpressionColumn : public Expression {
  int columnId;
 public:
  ExpressionColumn(int id): columnId(id) { }
  Column* pull(vector<Column*>* sources) {
    return (*sources)[columnId];
  }

  std::ostream& debugPrint(std::ostream& output) {
    return output << "ExpressionColumn(" << columnId << ")";
  }

  int getType() {
    return global::getType<T>();
  }
};

template<class T>
class ExpressionAdd : public Expression2<T> {
 public:
  ExpressionAdd(Expression* l, Expression* r): Expression2<T>(l, r) { }

  void pullInt(Column* a, Column* b);
};

template<>
inline void
ExpressionAdd<int>::pullInt(Column* a, Column* b) {
  int* aT = ((ColumnChunk<int>*) a)->chunk;
  int* bT = ((ColumnChunk<int>*) b)->chunk;
  int* target = cache.chunk;

  cache.size = a->size;

  for (int i = 0 ; i < cache.size ; ++i) {
    target[i] = aT[i] + bT[i];
  }
}

template<>
inline void
ExpressionAdd<double>::pullInt(Column* a, Column* b) {
  cache.size = a->size;
  double* target = cache.chunk;

  if (dynamic_cast<ColumnChunk<double>*>(a) == NULL ||
      dynamic_cast<ColumnChunk<double>*>(b) == NULL
      ) {
    if (dynamic_cast<ColumnChunk<double>*>(a) == NULL) {
      swap(a, b);
    }
    double *aT = ((ColumnChunk<double>*) a)->chunk; 
    int *bT = ((ColumnChunk<int>*) b)->chunk; 

    for (int i = 0 ; i < cache.size ; ++i) {
      target[i] = aT[i] + bT[i];
    }
  } else {
    double *aT = ((ColumnChunk<double>*) a)->chunk; 
    double *bT = ((ColumnChunk<double>*) b)->chunk; 

    for (int i = 0 ; i < cache.size ; ++i) {
      target[i] = aT[i] + bT[i];
    }
  }
}

template<>
inline void
ExpressionAdd<char>::pullInt(Column* a, Column* b) {
  assert(false);
}


#endif
