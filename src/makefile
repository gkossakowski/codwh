INC=-Ibuild/ -I../build/ -Iserver/ -I. -I/opt/local/include/
CC=g++ -g -Wall ${INC}
OBJS=build/proto.o build/server.o \
		 build/operators/node.o build/groupby.o \
		 build/operators/factory.o
LIBS=-lprotobuf -lpthread -L/opt/local/lib/ -lboost_program_options

exec_plan: database
	cp $< $@

database: main.cc ${OBJS}
	${CC} ${OBJS} ${LIBS} -o $@ $<

build/operators/%.o: operators/%.cc operators/%.h
	mkdir -p build/operators
	${CC} -c -o $@ $<

build/groupby.o: operators/groupby.cc
	mkdir -p build/
	${CC} -c -o $@ $<

build/server.o: server.cc server/server.h
	mkdir -p build/
	${CC} -c -o $@ $<

build/proto.o: build/proto/operations.pb.cc
	${CC} -c -o $@ $<

build/proto/operations.pb.cc: proto/operations.proto
	mkdir -p build/
	protoc $< --cpp_out build/

clean:
	rm -rf build/
