#include <thread>
#include <assert.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <boost/smart_ptr/detail/spinlock.hpp>
#include "FifoQueue.h"

#include <libmemcached/memcached.h>
#define PRIVATE private
#include "../memcached-1.4.15-cleaner/Cycles.h"

int VALUE_LENGTH = 25;

// If true, when we do a get() and it isn't the expected length, do a new
// set with the new length. This simulates updating the cache when software
// adds a new field.
bool UPDATE_CHANGED_VALUE_LENGTH = true;

// XXX Not reason why this is should a separate flag from the '-s' parameter, no?
bool USE_LENGTH_FROM_FILE = true;

static char randomChars[100000];

#define MEMCACHED_THREADS 16

std::atomic<uint64_t> getAttempts(0);
std::atomic<uint64_t> getFailures(0);
std::atomic<uint64_t> setAttempts(0);
std::atomic<uint64_t> setFailures(0);
uint32_t linesProcessed = 0;

// Set to true to cause memcached worker threads to quit
static volatile bool threadsQuit = false;

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
boost::detail::spinlock queueLock;

void
issueSet(memcached_st* memc, char* key, int valueLen)
{
    assert(valueLen <= (int)sizeof(randomChars));
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
        if (UPDATE_CHANGED_VALUE_LENGTH && (int)valueLength != VALUE_LENGTH) {
            getFailures++;
            issueSet(memc, key, VALUE_LENGTH);
        }
        free(ret);
    }
}

void
memcachedThread()
{
    memcached_st* memc = memcached_create(NULL);
    memcached_return rc;
#if 0
    rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "failed to set binary protocol\n");
        exit(1);
    }

    rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_USE_UDP, 1);
    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "failed to set udp protocol\n");
        exit(1);
    }
#endif
    rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "failed to set non-blocking IO\n");
        exit(1);
    }

    memcached_server_st* servers = memcached_server_list_append(NULL, "127.0.0.1", 11211, &rc);
    if (servers == NULL) {
        fprintf(stderr, "memcached_server_list_append failed: %d\n", (int)rc);
        exit(1);
    }
    rc = memcached_server_push(memc, servers);
    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "memcached_server_push failed: %d (%s)\n", (int)rc, memcached_strerror(memc, rc));
        exit(1);
    }

    while (!threadsQuit) {
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

    fprintf(stderr, "memcached worker thread exiting\n");
}

int
getValueLength(char* line)
{
    if (USE_LENGTH_FROM_FILE) {
        // XXX- this only works if a single field is given
        if (strchr(line, '[') != NULL)
            return (int)(strchr(line, ']') - strchr(line, '[')) - 10;

        // if there are no fields explicitly listed, then assume this has been run
        // through ycsb_munge.py and the key was stripped in favour of its byte length
        int length;
        sscanf(line, "%*s %*s %*s %d\n", &length);
        return length;
    }

    return VALUE_LENGTH;
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
        linesProcessed++;
    } else if (line[0] == 'I') {
        queue.push(Operation());
        queue.back().type = Operation::SET;
        sscanf(line, "INSERT usertable %s [", queue.back().key);
        queue.back().valueLength = getValueLength(line);
        linesProcessed++;
    } else if (line[0] == 'U') {
        queue.push(Operation());
        queue.back().type = Operation::SET;
        sscanf(line, "UPDATE usertable %s [", queue.back().key);
        queue.back().valueLength = getValueLength(line);
        linesProcessed++;
    }

    queueLock.unlock();
}

int
main(int argc, char** argv)
{
    int opt;
    char* progname = argv[0];
    uint32_t periodicity = 100000;

    while ((opt = getopt(argc, argv, "fP:s:")) != -1) {
        switch (opt) {
        case 'f':
            USE_LENGTH_FROM_FILE = false;
            break;
        case 'P':
            periodicity = atoi(optarg);
            break;
        case 's':
            VALUE_LENGTH = atoi(optarg);
            break;
        }
    }
    argc -= optind;
    argv += optind;

    if (argc < 1) {
        fprintf(stderr, "usage: %s ycsb-basic-workload-dump [...]\n", progname);
        exit(1);
    }

    for (int i = 0; i < (int)sizeof(randomChars); i++)
        randomChars[i] = '!' + (random() % ('~' - '!' + 1));

    printf("#spinning %d memcached worker threads\n", MEMCACHED_THREADS);
    std::thread* threads[MEMCACHED_THREADS];
    for (int i = 0; i < MEMCACHED_THREADS; i++)
        threads[i] = new std::thread(memcachedThread);

    printf("# UPDATE_CHANGED_VALUE_LENGTH = %s\n", (UPDATE_CHANGED_VALUE_LENGTH) ? "true" : "false");
    printf("# USE_LENGTH_FROM_FILE = %s\n", (USE_LENGTH_FROM_FILE) ? "true" : "false");
    printf("# VALUE_LENGTH = %d (ONLY APPLIES IF !USE_LENGTH_FROM_FILE)\n", VALUE_LENGTH);

    char buf[1000];
    uint64_t start = RAMCloud::Cycles::rdtsc();
    uint64_t lastGetAttempts = 0;
    uint64_t lastGetFailures = 0;
    uint64_t lastSetAttempts = 0;
    uint64_t lastSetFailures = 0;
    uint32_t lastLinesProcessed = 0;

    while (argc > 0) {
        printf("# Using workload file [%s]\n", argv[0]);
        FILE* fp = fopen(argv[0], "r");
        assert(fp != NULL);
        while (fgets(buf, sizeof(buf), fp) != NULL) {
            handleOp(buf);

            if ((linesProcessed - lastLinesProcessed) == periodicity) {
                    lastLinesProcessed = linesProcessed;
#if 0
                    printf("----------------------\n");
                    printf("Get Attempts: %e\n", (double)getAttempts);
                    printf("    Failures: %e  (%.5f%% misses)\n", (double)getFailures, (double)getFailures / (double)getAttempts * 100);
                    printf("    /sec:     %lu\n", (uint64_t)(((double)getAttempts) / elapsed));
                    printf("    Fails Last %.0fs:  %e  (%.5f%% misses)\n",
                        outputInterval,
                        (double)(getFailures - lastGetFailures),
                        (double)(getFailures - lastGetFailures) / (double)(getAttempts - lastGetAttempts) * 100);
                    
                    printf("Set Attempts: %e\n", (double)setAttempts);
                    printf("    Failures: %e  (%.5f%% failures)\n", (double)setFailures, (double)setFailures / (double)setAttempts * 100);
                    printf("    /sec:     %lu\n", (uint64_t)(((double)setAttempts) / elapsed));
                    printf("    Fails Last %.0fs:  %e  (%.5f%% of attempts)\n",
                        outputInterval
                        (double)(setFailures - lastSetFailures),
                        (double)(setFailures - lastSetFailures) / (double)(setAttempts - lastSetAttempts) * 100);
#endif
                    double elapsed = RAMCloud::Cycles::toSeconds(RAMCloud::Cycles::rdtsc() - start);
                    printf("%-10u lines   %.1f s   %e ops   %.5f%% misses   %.5f%% recently    %e set failures\n",
                        linesProcessed,
                        elapsed,
                        (double)(getAttempts + setAttempts),
                        (double)getFailures / (double)getAttempts * 100,
                        (double)(getFailures - lastGetFailures) / (double)(getAttempts - lastGetAttempts)* 100, (double)setFailures);
                    fflush(stdout);
                    lastGetAttempts = getAttempts;
                    lastGetFailures = getFailures;
                    lastSetAttempts = setAttempts;
                    lastSetFailures = setFailures;
            }
        }
        argc--;
        argv++;
        VALUE_LENGTH *= 2;
    }

    threadsQuit = true;
    for (int i = 0; i < MEMCACHED_THREADS; i++)
        threads[i]->join();

    return 0;
}
