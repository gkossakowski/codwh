INC=-Ibuild/ -I../build/ -Iserver/ -I. -I/opt/local/include/
CC=g++ -g -rdynamic -Wall -ggdb ${INC} ${FLAGS}
OBJS=build/proto.o build/server.o \
		 build/operators/node.o build/groupby.o \
		 build/operators/factory.o \
		 build/operators/operation.o \
		 build/node_environment/libnode_environment.a \
	 	 build/utils/libutils.a \
	 	 build/netio/libnetio.a \
		 build/distributed_node.o \
		 build/distributed_scheduler.o 
NET_OBJS= build/proto.o \
		 build/operators/node.o \
		 build/groupby.o \
		 build/operators/factory.o \
		 build/operators/operation.o \
		 build/node_environment/libnode_environment.a \
	 	 build/utils/libutils.a \
	 	 build/netio/libnetio.a \
		 build/distributed_node.o \
		 build/distributed_scheduler.o
		 
LIBS=-lprotobuf -lpthread -L/opt/local/lib/ -lboost_program_options

# Ugly hack for an older version of boost on `students`
ifneq (,$(wildcard /usr/lib64/libboost_thread.so))
	BOOST_THREAD=-lboost_thread
else
	BOOST_THREAD=-lboost_thread-mt
endif
ifneq (,$(wildcard /usr/lib64/libboost_system.so))
	BOOST_SYSTEM=-lboost_system
else
	BOOST_SYSTEM=-lboost_system-mt
endif
NET_LIBS=${LIBS} ${BOOST_THREAD} ${BOOST_SYSTEM}


all: exec_plan scheduler worker

exec_plan: main.cc ${OBJS}
	${CC} ${OBJS} $< ${LIBS} ${NET_LIBS} -o $@

build/operators/%.o: operators/%.cc operators/%.h operators/column.h
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

build/distributed_node.o: distributed/node.cc distributed/node.h  build/proto.o
	mkdir -p build/
	${CC} -c -o $@ $<

build/distributed_scheduler.o: distributed/scheduler.cc distributed/node.h  build/proto.o
	mkdir -p build/
	${CC} -c -o $@ $<

worker: worker.cc build/proto.o ${NET_OBJS}
	${CC} $< ${NET_OBJS} ${NET_LIBS} -o $@

scheduler: scheduler.cc build/proto.o ${NET_OBJS}
	${CC} $< ${NET_OBJS} ${NET_LIBS} -o $@

dist_tests: dist_tests.cc ${NET_OBJS}
	${CC} $< ${NET_OBJS} ${NET_LIBS} -o $@

# Netio
build/netio/network_input.o: netio/network_input.cc netio/network_input.h
	mkdir -p build/netio
	${CC} -c -o $@ $<

build/netio/network_output.o: netio/network_output.cc netio/network_output.h
	mkdir -p build/netio
	${CC} -c -o $@ $<

build/netio/libnetio.a: build/netio/network_input.o build/netio/network_output.o
	mkdir -p build/netio
	ar cru build/netio/libnetio.a build/netio/network_input.o build/netio/network_output.o
	ranlib build/netio/libnetio.a

# Utils
build/utils/ip_address.o: utils/ip_address.cc utils/ip_address.h
	mkdir -p build/utils
	${CC} -c -o $@ $<

build/utils/libutils.a: build/utils/ip_address.o
	mkdir -p build/utils
	ar cru build/utils/libutils.a build/utils/ip_address.o
	ranlib build/utils/libutils.a

# Node Environment
build/node_environment/node_environment.o: node_environment/node_environment.cc node_environment/node_environment.h
	mkdir -p build/node_environment
	${CC} -c -o $@ $<

build/node_environment/data_server.o: node_environment/data_server.cc node_environment/data_server.h
	mkdir -p build/node_environment
	${CC} -c -o $@ $<

build/node_environment/libnode_environment.a: build/node_environment/node_environment.o build/node_environment/data_server.o
	mkdir -p build/node_environment
	ar cru build/node_environment/libnode_environment.a build/node_environment/node_environment.o build/node_environment/data_server.o
	ranlib build/node_environment/libnode_environment.a

clean:
	rm -rf build/

prune: clean
	rm -f exec_plan


.PHONY: clean prune
