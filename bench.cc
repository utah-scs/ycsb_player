#include <cinttypes>
#include <iostream>
#include <vector>
#include <atomic>
#include <cassert>
#include <string>

#include <unistd.h>

#include <libmemcached/memcached.h>

#include "Benchmark.h"

// If true, when we do a get() and it isn't the expected length, do a new
// set with the new length. This simulates updating the cache when software
// adds a new field.
bool UPDATE_CHANGED_VALUE_LENGTH = true;

static thread_local PRNG prng{};

class SmallFillThenRead : public Benchmark {
  const size_t valueLen;
  const size_t nKeys;

  std::atomic<uint64_t> getAttempts;
  std::atomic<uint64_t> getFailures;
  std::atomic<uint64_t> setAttempts;
  std::atomic<uint64_t> setFailures;

  uint64_t lastGetAttempts;
  uint64_t lastGetFailures;

  char randomChars[100000];

  void issueSet(memcached_st* memc,
                const char* key,
                size_t valueLen)
  {
    assert(valueLen <= sizeof(randomChars));
    const char* value =
      &randomChars[prng()  % (sizeof(randomChars) - valueLen)];
    setAttempts++;
    memcached_return rc =
      memcached_set(memc, key, strlen(key), value, valueLen,
                    (time_t)0, (uint32_t)0);
    if (rc != MEMCACHED_SUCCESS)
      setFailures++;
  }

  void issueGet(memcached_st* memc, const char* key, size_t reinsertValueLen) {
    memcached_return rc;
    uint32_t flags;
    size_t valueLength;

    getAttempts++;

    char* ret = memcached_get(memc, key, strlen(key), &valueLength, &flags, &rc);
    if (ret == NULL) {
      getFailures++;

      // should just be a cache miss. handle by adding it to the cache.
      if (rc == MEMCACHED_NOTFOUND) { 
        issueSet(memc, key, reinsertValueLen);
      } else {
        std::cerr << "unexpected get error: " <<  memcached_strerror(memc, rc)
                  << std::endl;
        exit(1);
      }
    } else {
      if (UPDATE_CHANGED_VALUE_LENGTH && valueLength != reinsertValueLen) {
        getFailures++;
        issueSet(memc, key, reinsertValueLen);
      }
      free(ret);
    }
  }

  void warmup(size_t threadId) {
    prng.reseed(threadId);
  }

  // Just do round-robin gets until time is up.
  void run(size_t threadId) {
    size_t key = 0;
    while (!getStop()) {
      const std::string keyStr = "user" + std::to_string(key);
      issueGet(getClient(threadId), keyStr.c_str(), valueLen);
      ++key;
      if (key > nKeys)
        key = 0;
    }
  }

  void dumpHeader() {
    std::cout << "time" << " "
              << "getAttempts" << " "
              << "getFailures" << " "
              << "setAttempts" << " "
              << "setFailures" << " "
              << "okGetsPerSec" << " "
              << std::endl;
  }

  void dump(double time, double interval) {
    const uint64_t intervalGetAttempts = getAttempts - lastGetAttempts;
    const uint64_t intervalGetFailures = getFailures - lastGetFailures;
    std::cout << time << " "
              << getAttempts << " "
              << getFailures << " "
              << setAttempts << " "
              << setFailures << " "
              << (intervalGetAttempts - intervalGetFailures) / interval << " "
              << std::endl;
    lastGetAttempts = getAttempts;
    lastGetFailures = getFailures;
  }

 public:
  SmallFillThenRead(size_t port, size_t nThreads, double seconds,
                    size_t valueLen, size_t nKeys)
    : Benchmark{port, nThreads, seconds}
    , valueLen{valueLen}
    , nKeys{nKeys}
    , getAttempts{}
    , getFailures{}
    , setAttempts{}
    , setFailures{}
    , lastGetAttempts{}
    , lastGetFailures{}
  {
    for (size_t i = 0; i < sizeof(randomChars); ++i)
      randomChars[i] = '!' + (random() % ('~' - '!' + 1));
  }

  // Do normal benchmark start stuff, but then also prefill with some data.
  // This happens on the main thread, so it happens just once in the order
  // specified.
  void start() {
    Benchmark::start();

    for (uint64_t key = 0; key < nKeys; ++key) {
      const std::string keyStr = "user" + std::to_string(key);
      issueSet(getClient(0), keyStr.c_str(), valueLen);
    }
  }
};

int main(int argc, char* argv[]) {
  size_t nThreads = 1;
  double seconds = 10.0;
  size_t valueLen = 1024;
  size_t nKeys = 10000;
  size_t port = 12000;

  int c;
  while ((c = getopt(argc, argv, "t:s:k:T:p:")) != -1) {
    switch (c)
    {
      case 't':
        seconds = std::stod(optarg);
        break;
      case 's':
        valueLen = std::stoul(optarg);
        break;
      case 'k':
        nKeys = std::stoul(optarg);
        break;
      case 'T':
        nThreads = std::stoul(optarg);
        break;
      case 'p':
        port = std::stoul(optarg);
        break;
      default:
        std::cerr << "Unknown argument" << std::endl;
        exit(-1);
    }
  }

  SmallFillThenRead bench{port, nThreads, seconds, valueLen, nKeys};
  printf(stdout, "nthreads: %d seconds: %f valuelen: %d nkeys: %d",
      nThreads, seconds, valueLen, nKeys);
  bench.start();

  return 0;
}
