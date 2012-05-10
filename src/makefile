INC=-Ibuild/ -I../build/ -Iserver/ -I. -I/opt/local/include/
CC=g++ -O2 -Wall ${INC}
OBJS=build/proto.o build/server.o \
		 build/operators/node.o build/groupby.o \
		 build/operators/factory.o \
		 build/operators/operation.o
LIBS=-lprotobuf -lpthread -L/opt/local/lib/ -lboost_program_options

all: exec_plan

exec_plan: main.cc ${OBJS}
	${CC} ${OBJS} $< ${LIBS} -o $@

build/operators/%.o: operators/%.cc operators/%.h
	mkdir -p build/operators
	${CC} -c -o $@ $<

build/groupby.o: operators/groupby.cc operators/operation.h
	mkdir -p build/
	${CC} -c -o $@ $<

build/server.o: server/server_real.cc server/server.h
	mkdir -p build/
	${CC} -c -o $@ $<

build/proto.o: build/proto/operations.pb.cc
	${CC} -c -o $@ $<

build/proto/operations.pb.cc: proto/operations.proto
	mkdir -p build/
	protoc $< --cpp_out build/

clean:
	rm -rf build/

prune: clean
	rm -f exec_plan


.PHONY: clean prune
