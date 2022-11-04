#ifndef __ASYNC_TASK_H__
#define __ASYNC_TASK_H__

#include "util/thread.h"
#include "util/lock.h"
#include "util/util.h"
#include "util/ringque.h"

#include "common/mongo_cli.h"

#include <string>
#include <vector>
#include <bitset>
#include <pthread.h>

#include "tiny-redis/server.h"

typedef struct MissTask {
    uint64_t ms;
    std::string key;
} MissTask;

namespace inv {
    class INV_CoRedis;
}

class ASyncTask : public ThreadBase
{
public:
    virtual ~ASyncTask();

    int init(int queSize, 
            const std::string& url, const std::string& db, const std::string& collection,
            const std::string& redisIP, int redisPort, int redisTimeout);

    virtual void run();

    virtual void stop();

protected:

    void ExecMongoTask(const std::string& key);

protected:
    ASyncTask() {}

    int m_stop;

    RingQue<MissTask>*  m_queue;
    pthread_mutex_t     m_lockQue;
    pthread_cond_t      m_condQue;

    MongoCli*           m_mongoCli;
    inv::INV_CoRedis*   m_redis;

public:
    static int Start(int n, int queSize, 
            const std::string& url, const std::string& db, const std::string& collection,
            const std::string& redisIP, int redisPort, int redisTimeout);

    static void Stop();

    static int PushTask(const char* key);

protected:
    static std::vector<ASyncTask*>  m_tasks;

    static std::bitset<0x10000>     m_filter;
};

#endif

