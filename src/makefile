INC=-Ibuild/
CC=g++ ${INC}

build/proto.o: build/proto/operations.pb.cc
		${CC} -c -o $@ $<

build/proto/operations.pb.cc: proto/operations.pb
		mkdir -p build/
		protoc $< --cpp_out build/

clean:
		rm -rf build/
