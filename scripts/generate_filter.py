ident = 0
def mPrint(line):
  global ident
  ident = ident - line.count('}')
  print ("  " * ident) + line
  ident = ident + line.count('{') + line.count('case ') - line.count('return')

mPrint("// File was generated by 'scripts/generate_filter.py'")
mPrint("// DO NOT MODIFY!")
mPrint("// Fast And Furious column database")
mPrint("// Author: Jacek Migdal <jacek@migdal.pl>")
mPrint("")

mPrint("#ifndef FILTER_H")
mPrint("#define FILTER_H")
mPrint("template<class T>")
mPrint("int filterPrimitive(T* target, T* source, unsigned char current, int rest) {")
mPrint("switch (current) {")
for i in xrange(256):
  mPrint("case 0x%x:" % i)
  bitCount = 0
  for bitI in xrange(8):
    bit = 1 << bitI
    if (i & bit) != 0:
      mPrint("target[rest + %d] = source[%d];" % (bitCount, bitI))
      bitCount += 1
  mPrint("return %d;" % bitCount)
mPrint("}")
mPrint("}")
mPrint("#endif /* FILTER_H */")