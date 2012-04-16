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
class Expression1 : public Expression {
 protected:
  ColumnChunk<T> cache;
  Expression* left;
  virtual void pullInt(Column* data) = 0;
  virtual string getName() { return ""; }

 public:
  Expression1(Expression* l): left(l) { }

  Column* pull(vector<Column*>* sources) {
    Column* leftData = left->pull(sources);
    cache.size = leftData->size;
    pullInt(leftData);
    return &cache;
  }

  int getType() {
    return global::getType<T>();
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << getName() << "(";
    left->debugPrint(output);
    return output << ")";
  }
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
class ExpressionConstant : public Expression {
  T val;
  ColumnChunk<T> chunk;
 public:
  ExpressionConstant(bool v) {
    if (v) {
      val = 0xff;
    } else {
      val = 0;
    }
    for (int i = 0 ; i < chunk.size ; ++i) {
      chunk.chunk[i] = val;
    }
  }
  ExpressionConstant(T v): val(v) {
    for (int i = 0 ; i < chunk.size ; ++i) {
      chunk.chunk[i] = val;
    }
  }
  Column* pull(vector<Column*>* sources) {
    if ((*sources).size() > 0) {
      chunk.size = (*sources)[0]->size;
    } else {
      chunk.size = DEFAULT_CHUNK_SIZE;
    }
    return &chunk;
  }

  std::ostream& debugPrint(std::ostream& output) {
    return output << "ExpressionConstant(" << val << ")";
  }

  int getType() {
    return global::getType<T>();
  }
};

// Add {{{
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
// }}}

// Minus {{{
template<class T>
class ExpressionMinus : public Expression2<T> {
 public:
  ExpressionMinus(Expression* l, Expression* r): Expression2<T>(l, r) { }

  void pullInt(Column* a, Column* b);
};

template<>
inline void
ExpressionMinus<int>::pullInt(Column* a, Column* b) {
  int* aT = ((ColumnChunk<int>*) a)->chunk;
  int* bT = ((ColumnChunk<int>*) b)->chunk;
  int* target = cache.chunk;

  cache.size = a->size;

  for (int i = 0 ; i < cache.size ; ++i) {
    target[i] = aT[i] - bT[i];
  }
}

template<>
inline void
ExpressionMinus<double>::pullInt(Column* a, Column* b) {
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
      target[i] = aT[i] - bT[i];
    }
  } else {
    double *aT = ((ColumnChunk<double>*) a)->chunk; 
    double *bT = ((ColumnChunk<double>*) b)->chunk; 

    for (int i = 0 ; i < cache.size ; ++i) {
      target[i] = aT[i] - bT[i];
    }
  }
}

template<>
inline void
ExpressionMinus<char>::pullInt(Column* a, Column* b) {
  assert(false);
}
// }}}

// Multiply {{{
template<class T>
class ExpressionMultiply : public Expression2<T> {
 public:
  ExpressionMultiply(Expression* l, Expression* r): Expression2<T>(l, r) { }

  void pullInt(Column* a, Column* b);
};

template<>
inline void
ExpressionMultiply<int>::pullInt(Column* a, Column* b) {
  int* aT = ((ColumnChunk<int>*) a)->chunk;
  int* bT = ((ColumnChunk<int>*) b)->chunk;
  int* target = cache.chunk;

  cache.size = a->size;

  for (int i = 0 ; i < cache.size ; ++i) {
    target[i] = aT[i] * bT[i];
  }
}

template<>
inline void
ExpressionMultiply<double>::pullInt(Column* a, Column* b) {
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
      target[i] = aT[i] * bT[i];
    }
  } else {
    double *aT = ((ColumnChunk<double>*) a)->chunk; 
    double *bT = ((ColumnChunk<double>*) b)->chunk; 

    for (int i = 0 ; i < cache.size ; ++i) {
      target[i] = aT[i] * bT[i];
    }
  }
}

template<>
inline void
ExpressionMultiply<char>::pullInt(Column* a, Column* b) {
  assert(false);
}
// }}}

// Divide {{{
template<class T>
class ExpressionDivide : public Expression2<T> {
 public:
  ExpressionDivide(Expression* l, Expression* r): Expression2<T>(l, r) { }

  void pullInt(Column* a, Column* b);
};

template<>
inline void
ExpressionDivide<int>::pullInt(Column* a, Column* b) {
  assert(false);
}

template<>
inline void
ExpressionDivide<double>::pullInt(Column* a, Column* b) {
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
      target[i] = aT[i] * bT[i];
    }
  } else if (dynamic_cast<ColumnChunk<double>*>(a) == NULL &&
      dynamic_cast<ColumnChunk<double>*>(b) == NULL
      ) {
    double *aT = ((ColumnChunk<double>*) a)->chunk; 
    double *bT = ((ColumnChunk<double>*) b)->chunk; 

    for (int i = 0 ; i < cache.size ; ++i) {
      target[i] = aT[i] * bT[i];
    }
  } else {
    int *aT = ((ColumnChunk<int>*) a)->chunk; 
    int *bT = ((ColumnChunk<int>*) b)->chunk; 

    for (int i = 0 ; i < cache.size ; ++i) {
      target[i] = aT[i] * bT[i];
    }
  }
}

template<>
inline void
ExpressionDivide<char>::pullInt(Column* a, Column* b) {
  assert(false);
}
// }}}

// Or, And, Not {{{
class ExpressionOr : public Expression2<char> {
 public:
  ExpressionOr(Expression* l, Expression* r): Expression2<char>(l, r) { }
  void pullInt(Column* a, Column* b) {
    cache.size = a->size;
    unsigned* aT = (unsigned*) static_cast<ColumnChunk<char>*>(a)->chunk;
    unsigned* bT = (unsigned*) static_cast<ColumnChunk<char>*>(b)->chunk;
    unsigned* target = (unsigned*) cache.chunk;
    int sizeInWords = (cache.size + 31) / 32;

    for (int i = 0 ; i < sizeInWords ; ++i) {
      target[i] = aT[i] | bT[i];
    }
  }
};

class ExpressionAnd : public Expression2<char> {
 public:
  ExpressionAnd(Expression* l, Expression* r): Expression2<char>(l, r) { }
  void pullInt(Column* a, Column* b) {
    cache.size = a->size;
    unsigned* aT = (unsigned*) static_cast<ColumnChunk<char>*>(a)->chunk;
    unsigned* bT = (unsigned*) static_cast<ColumnChunk<char>*>(b)->chunk;
    unsigned* target = (unsigned*) cache.chunk;
    int sizeInWords = (cache.size + 31) / 32;

    for (int i = 0 ; i < sizeInWords ; ++i) {
      target[i] = aT[i] & bT[i];
    }
  }
};

class ExpressionNot : public Expression1<char> {
 public:
  ExpressionNot(Expression* l): Expression1<char>(l) { }
  void pullInt(Column* a) {
    unsigned* aT = (unsigned*) static_cast<ColumnChunk<char>*>(a)->chunk;
    unsigned* target = (unsigned*) cache.chunk;
    int sizeInWords = (cache.size + 31) / 32;

    for (int i = 0 ; i < sizeInWords ; ++i) {
      target[i] = ~aT[i];
    }
  }
};
// }}}

// Log {{{
template<class T>
class ExpressionLog : public Expression {
  ColumnChunk<double> cache;
  Expression* left;
 public:
  ExpressionLog(Expression* l): left(l) { }
  Column* pull(vector<Column*>* sources) {
    Column* a = left->pull(sources);
    T* source = static_cast<ColumnChunk<T>*>(a)->chunk;
    double* target = cache.chunk;
    int n = cache.size = a->size;

    for (int i = 0 ; i < n ; ++i) {
      target[i] = log(source[i]);
    }

    return &cache;
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << "ExpressionLog { ";
    left->debugPrint(output);
    return output << "}";
  }

  int getType() {
    return global::getType<double>();
  }
};
// }}}

// Negate {{{
template<class T>
class ExpressionNegate : public Expression1<T> {
 public:
  ExpressionNegate(Expression* l): Expression1<T>(l) { }
  void pullInt(Column* a);
};

template<>
inline void
ExpressionNegate<int>::pullInt(Column* a) {
  int* target = cache.chunk;
  int* aT = static_cast<ColumnChunk<int>*>(a)->chunk;

  for (int i = 0 ; i < cache.size ; ++i) {
    target[i] = -aT[i];
  }
}

template<>
inline void
ExpressionNegate<double>::pullInt(Column* a) {
  double* target = cache.chunk;
  double* aT = static_cast<ColumnChunk<double>*>(a)->chunk;

  for (int i = 0 ; i < cache.size ; ++i) {
    target[i] = -aT[i];
  }
}

template<>
inline void
ExpressionNegate<char>::pullInt(Column* a) {
  assert(false);
}
// }}}

// Lower, Equal, not equal {{{
template<class T>
class ExpressionLogic : public Expression {
 protected:
  Expression* left;
  Expression* right;
  ColumnChunk<char> result;
  virtual void pullLogic(T* aT, T* bT, char* target, int size) = 0;
 public:
  ExpressionLogic(Expression* l, Expression* r): left(l), right(r) { }
  Column* pull(vector<Column*>* sources) {
    Column* a = left->pull(sources);
    Column* b = right->pull(sources);
    result.size = a->size;
    T* aT = static_cast<ColumnChunk<T>*>(a)->chunk;
    T* bT = static_cast<ColumnChunk<T>*>(b)->chunk;
    char* target = &result.chunk[0];
    pullLogic(aT, bT, target, result.size);
    return &result;
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << "ExpressionLogic { ";
    left->debugPrint(output);
    output << " ";
    right->debugPrint(output);
    return output << "}\n";
  }

  int getType() {
    return global::getType<T>();
  }
};

template<class T>
class ExpressionLower : public ExpressionLogic<T> {
 protected:
  void pullLogic(T* aT, T* bT, char* target, int size) {
    int n = (size + 7) / 8;
    for(int i = 0 ; i < n ; ++i) {
      target[i] = 0;
    }
    for(int i = 0 ; i < size ; ++i) {
      if (aT[i] < bT[i])
        target[i / 8] |= 1 << (i & 7);
    }
  }
 public:
  ExpressionLower(Expression* l, Expression* r): ExpressionLogic<T>(l, r) { }
};

template<class T>
class ExpressionEqual : public ExpressionLogic<T> {
 protected:
  void pullLogic(T* aT, T* bT, char* target, int size) {
    int n = (size + 7) / 8;
    for(int i = 0 ; i < n ; ++i) {
      target[i] = 0;
    }
    for(int i = 0 ; i < size ; ++i) {
      if (aT[i] == bT[i])
        target[i / 8] |= 1 << (i & 7);
    }
  }
 public:
  ExpressionEqual(Expression* l, Expression* r): ExpressionLogic<T>(l, r) { }
};

template<>
inline void
ExpressionEqual<char>::pullLogic(char* aT, char* bT, char* target, int size) {
  int n = (size + 7) / 8;
  for(int i = 0 ; i < n ; ++i) {
    target[i] = ~(aT[i] ^ bT[i]);
  }
}

template<class T>
class ExpressionNotEqual : public ExpressionLogic<T> {
 protected:
  void pullLogic(T* aT, T* bT, char* target, int size) {
    int n = (size + 7) / 8;
    for(int i = 0 ; i < n ; ++i) {
      target[i] = 0;
    }
    for(int i = 0 ; i < size ; ++i) {
      if (aT[i] != bT[i])
        target[i / 8] |= 1 << (i & 7);
    }
  }
 public:
  ExpressionNotEqual(Expression* l, Expression* r): ExpressionLogic<T>(l, r) { }
};

template<>
inline void
ExpressionNotEqual<char>::pullLogic(char* aT, char* bT, char* target, int size) {
  int n = (size + 7) / 8;
  for(int i = 0 ; i < n ; ++i) {
    target[i] = aT[i] ^ bT[i];
  }
}
// }}}

// If {{{
template<class T>
class ExpressionIf : public Expression {
 protected:
  Expression* condition;
  Expression* left;
  Expression* right;
  ColumnChunk<T> result;
 public:
  ExpressionIf(Expression* cond, Expression* l, Expression* r):
    condition(cond), left(l), right(r) { }
  Column* pull(vector<Column*>* sources) {
    Column* cond = condition->pull(sources);
    Column* a = left->pull(sources);
    Column* b = right->pull(sources);
    result.size = a->size;
    unsigned char* cT = (unsigned char*) static_cast<ColumnChunk<char>*>(cond)->chunk;
    T* aT = static_cast<ColumnChunk<T>*>(a)->chunk;
    T* bT = static_cast<ColumnChunk<T>*>(b)->chunk;
    T* target = &result.chunk[0];

    for (int i = 0 ; i < result.size ; ++i) {
      if (cT[i / 8] & (1 << (i & 7))) {
        target[i] = aT[i];
      } else {
        target[i] = bT[i];
      }
    }

    return &result;
  }

  std::ostream& debugPrint(std::ostream& output) {
    output << "ExpressionIf{ ";
    condition->debugPrint(output);
    output << ": ";
    left->debugPrint(output);
    output << " ";
    right->debugPrint(output);
    return output << "}\n";
  }

  int getType() {
    return global::getType<T>();
  }
};
// }}}


#endif
