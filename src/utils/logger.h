/*
 * logger.h
 *
 *  Created on: 26-04-2012
 *      Author: ptab
 */

#ifndef LOGGER_H_
#define LOGGER_H_

#include <stdlib.h>

#define CHECK(x, msg) if (!(x)) { fprintf(stderr, "%s:%d Assertion failed: %s; %s\n", __FILE__, __LINE__, #x, msg); exit(1); };

#define LOG1(str, x1) //printf(str "\n", x1);
#define LOG2(str, x1, x2) //printf(str "\n", x1, x2);
#define LOG3(str, x1, x2, x3) printf(str "\n", x1, x2, x3);
#define LOG4(str, x1, x2, x3, x4) //printf(str "\n", x1, x2, x3, x4);

#endif /* LOGGER_H_ */
