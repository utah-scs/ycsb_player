#include <thread>
#include <assert.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "FifoQueue.h"

#include <libmemcached/memcached.h>
#define PRIVATE private
#include "../memcached-1.4.15-cleaner/Cycles.h"

#define VALUE_LENGTH 25

static char randomChars[100000];

#define MEMCACHED_THREADS 8

std::atomic<uint64_t> getAttempts(0);
std::atomic<uint64_t> getFailures(0);
std::atomic<uint64_t> setAttempts(0);
std::atomic<uint64_t> setFailures(0);

class Operation {
  public:
    enum OperationType {
        INVALID,
        GET, 
        SET
    };

    Operation()
        : type(INVALID)
        , key()
        , valueLength(0)
    {
    }

    enum OperationType type;
    char key[100];
    size_t valueLength;
};

#define MAX_QUEUE_LENGTH 1000
FifoQueue<Operation> queue;
std::mutex queueLock;

void
issueSet(memcached_st* memc, char* key, int valueLen)
{
    assert(valueLen <= sizeof(randomChars));
    char* value = &randomChars[random() % (sizeof(randomChars) - valueLen)];

    setAttempts++;
    memcached_return rc = memcached_set(memc, key, strlen(key), value, valueLen, (time_t)0, (uint32_t)0);
    if (rc != MEMCACHED_SUCCESS) {
        //fprintf(stderr, "set rc == %d (%s)\n", (int)rc, memcached_strerror(memc, rc));
        setFailures++;
    }
}

void
issueGet(memcached_st* memc, char* key)
{
    memcached_return rc;
    uint32_t flags;
    size_t valueLength;

    getAttempts++;
    char* ret = memcached_get(memc, key, strlen(key), &valueLength, &flags, &rc);
    if (ret == NULL) {
        //fprintf(stderr, "get rc == %d (%s)\n", (int)rc, memcached_strerror(memc, rc));
        getFailures++;

        // should just be a cache miss. handle by adding it to the cache.
        if (rc == MEMCACHED_NOTFOUND) { 
            issueSet(memc, key, VALUE_LENGTH);
        } else {
            fprintf(stderr, "unexpected get error: %s\n", memcached_strerror(memc, rc));
            exit(1);
        }
    } else {
        free(ret);
    }
}

void
memcachedThread()
{
    memcached_st* memc = memcached_create(NULL);
    memcached_return rc;
    memcached_server_st* servers = memcached_server_list_append(NULL, "127.0.0.1", 11211, &rc);
    if (servers == NULL) {
        fprintf(stderr, "memcached_server_list_append failed: %d\n", (int)rc);
        exit(1);
    }
    rc = memcached_server_push(memc, servers);
    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "memcached_server_push failed: %d\n", (int)rc);
        exit(1);
    }

    while (1) {
        queueLock.lock();
        if (queue.empty()) {
            queueLock.unlock();
            continue;
        }

        Operation op = queue.pop();
        queueLock.unlock();

        if (op.type == Operation::GET) {
            issueGet(memc, op.key);
        } else if (op.type == Operation::SET) {
            issueSet(memc, op.key, op.valueLength);
        } else {
            fprintf(stderr, "invalid operation!\n");
            exit(1);
        }
    }
}

// this method can parse through about 4M operations/sec from the ycsb
// text output
void
handleOp(char *line)
{
    /*
     * READ usertable user6622674881006267921 [ <all fields>]
     * INSERT usertable user8183854946431771896 [ field0=8#?(;?4%4*'4#0$"=/$*9"/)-!?36?7#>8>"-0$&2(2"0+))  &'-;+7 ()7%->56.!;2<086;-!#.9067 01(=!%3<$;7$#7#,; ]
     */

    bool queueFull = true;
    while (queueFull) {
        queueLock.lock();
        queueFull = (queue.size() == MAX_QUEUE_LENGTH);
        queueLock.unlock();
        if (queueFull)
            usleep(100);
    }

    queueLock.lock();

    if (line[0] == 'R') {
        queue.push(Operation());
        queue.back().type = Operation::GET;
        sscanf(line, "READ usertable %s [", queue.back().key);
    } else if (line[0] == 'I') {
        queue.push(Operation());
        queue.back().type = Operation::SET;
        sscanf(line, "INSERT usertable %s [", queue.back().key);
        queue.back().valueLength = VALUE_LENGTH;

        // XXX- this only works if a single field is given
        //valueLen = (int)(strchr(line, ']') - strchr(line, '[')) - 10;
    }

    queueLock.unlock();
}

int
main(int argc, char** argv)
{
    if (argc != 2) {
        fprintf(stderr, "usage: %s ycsb-basic-workload-dump\n", argv[0]);
        exit(1);
    }

    for (int i = 0; i < sizeof(randomChars); i++)
        randomChars[i] = '!' + (random() % ('~' - '!' + 1));

    printf("spinning %d memcached worker threads\n", MEMCACHED_THREADS);
    for (int i = 0; i < MEMCACHED_THREADS; i++)
        new std::thread(memcachedThread);

    char buf[1000];
    FILE* fp = fopen(argv[1], "r");
    uint64_t start = RAMCloud::Cycles::rdtsc();
    uint64_t lastOutput = start;
    uint32_t chill = 0;
    while (fgets(buf, sizeof(buf), fp) != NULL) {
        handleOp(buf);

        if ((chill++ & 0xff) == 0) {
            uint64_t now = RAMCloud::Cycles::rdtsc();
            double elapsed = RAMCloud::Cycles::toSeconds(now - start);
            double sinceLastOutput = RAMCloud::Cycles::toSeconds(now - lastOutput);
            if (sinceLastOutput >= 5) {
                printf("----------------------\n");
                printf("Get Attempts: %e\n", (double)getAttempts);
                printf("    Failures: %e  (%.5f%% misses)\n", (double)getFailures, (double)getFailures / (double)getAttempts * 100);
                printf("    /sec:     %lu\n", (uint64_t)(((double)getAttempts) / elapsed));
                
                printf("Set Attempts: %e\n", (double)setAttempts);
                printf("    Failures: %e  (%.5f%% failures)\n", (double)setFailures, (double)setFailures / (double)setAttempts * 100);
                printf("    /sec:     %lu\n", (uint64_t)(((double)setAttempts) / elapsed));
                lastOutput = now;
            }
        }
    }

    return 0;
}
