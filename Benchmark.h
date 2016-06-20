#include <cinttypes>
#include <vector>
#include <thread>
#include <atomic>

#ifndef BENCHMARK_H
#define BENCHMARK_H

class memcached_st;

class Benchmark {
 public:
  Benchmark(size_t port, size_t nThreads, double seconds);
  ~Benchmark();

  memcached_st* getClient(size_t threadId) { return clients.at(threadId); }
  bool getStop() { return stop; }

  virtual void start();

 private:
  virtual void warmup(size_t threadId) {}
  virtual void run(size_t threadId) = 0;
  virtual void dump(double time) {}

  void entry(size_t threadId);

  const size_t port;
  const size_t nThreads;
  const double seconds;

  std::vector<memcached_st*> clients;
  std::vector<std::thread> threads;

  std::atomic<size_t> nReady;
  std::atomic<bool> go;
  std::atomic<bool> stop;
  std::atomic<size_t> nDone;
};

#endif
