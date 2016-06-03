ycsb_player: ycsb_player.cc
	g++ -Wall -std=gnu++11 -O3 -g -o ycsb_player ycsb_player.cc ../memcached-1.4.15-cleaner/Cycles.cc -lmemcached -I/home/stutsman/memcached/usr/include -L/home/stutsman/memcached/usr/lib -lpthread

clean:
	rm -f ycsb_player
