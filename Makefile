ycsb_player: ycsb_player.cc
	g++ -Wall -std=gnu++11 -O3 -g -o ycsb_player ycsb_player.cc Cycles.cc -lmemcached -lpthread

clean:
	rm -f ycsb_player
