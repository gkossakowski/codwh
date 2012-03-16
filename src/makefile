INC=-Ibuild/
CC=g++ ${INC}
OBJS=build/proto.o build/operators/constant_operator.o
LIBS=-lprotobuf -lpthread

database: main.cc ${OBJS}
	${CC} ${OBJS} ${LIBS} -o $@ $<

build/operators/%.o: operators/%.cc operators/%.h
	mkdir -p build/operators
	${CC} -c -o $@ $<

build/proto.o: build/proto/operations.pb.cc
		${CC} -c -o $@ $<

build/proto/operations.pb.cc: proto/operations.proto
		mkdir -p build/
		protoc $< --cpp_out build/

clean:
		rm -rf build/
