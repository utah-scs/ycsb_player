#include "Benchmark.h"

#include <iostream>

#include <libmemcached/memcached.h>
#include "Cycles.h"

using RAMCloud::Cycles;

Benchmark::Benchmark(size_t port, size_t nThreads, double seconds)
  : port{port}
  , nThreads{nThreads}
  , seconds{seconds}
  , clients{}
  , threads{}
  , nReady{}
  , go{}
  , stop{}
  , nDone{}
{
}

Benchmark::~Benchmark()
{
  for (auto& memc : clients)
    memcached_free(memc);
}

void
Benchmark::start()
{
  for (size_t i = 0; i < nThreads; ++i) {
    memcached_st* memc = memcached_create(NULL);

    memcached_return rc =
      memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
    if (rc != MEMCACHED_SUCCESS) {
      std::cerr << "failed to set non-blocking IO" << std::endl;
      exit(1);
    }

    memcached_server_st* servers =
      memcached_server_list_append(NULL, "127.0.0.1", port, &rc);
    if (servers == NULL) {
      std::cerr << "memcached_server_list_append failed: " << (int)rc
                << std::endl;
        exit(1);
    }
    rc = memcached_server_push(memc, servers);
    if (rc != MEMCACHED_SUCCESS) {
      std::cerr << "memcached_server_push failed: " << (int)rc << " "
                <<  memcached_strerror(memc, rc) << std::endl;
        exit(1);
    }

    clients.emplace_back(memc);
  }

  for (size_t i = 0; i < nThreads; ++i)
    threads.emplace_back(&Benchmark::entry, this, i);

  while (nReady < nThreads)
    std::this_thread::yield();
  go = true;

  using namespace std::chrono_literals;
  uint64_t start = Cycles::rdtsc();
  uint64_t endTs = start + Cycles::fromSeconds(seconds);
  uint64_t nextDumpTs = start + Cycles::fromSeconds(1.0);

  while (true) {
    uint64_t now = Cycles::rdtsc();
    if (nextDumpTs < now) {
      dump(Cycles::toSeconds(now - start));
      nextDumpTs = nextDumpTs + Cycles::fromSeconds(1.0);
    }
    if (endTs < now || nDone == nThreads)
      break;
    std::this_thread::sleep_for(1ms);
  }

  stop = true;

  for (auto& thread : threads)
    thread.join();
}

void
Benchmark::entry(size_t threadId)
{
  warmup(threadId);

  ++nReady;
  while (!go)
    std::this_thread::yield();

  run(threadId);

  ++nDone;
}

