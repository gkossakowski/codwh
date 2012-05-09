
all: compile

perf: compile
	./test_perf.sh

compile:
	make -C src clean
	make -C src
