all: ycsb_player bench

ycsb_player: ycsb_player.cc
	g++ -Wall -std=gnu++11 -O3 -g -o ycsb_player ycsb_player.cc Cycles.cc -lmemcached -lpthread

bench: bench.cc Cycles.h Benchmark.cc Benchmark.h
	g++ -Wall -Wpedantic -std=c++14 -O3 -g -o bench bench.cc Benchmark.cc Cycles.cc -lmemcached -lpthread

clean:
	rm -f ycsb_player bench
