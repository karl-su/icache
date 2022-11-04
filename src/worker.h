#ifndef __WORKER_H__
#define __WORKER_H__

#include "util/thread.h"

#include "tiny-redis/anet.h"
#include "tiny-redis/server.h"

class Worker : public ThreadBase
{
public:
    int init(TinyRedisDB* db);

    virtual void run();

    virtual void stop();

    TinyRedisProc* redis() { return m_redis; }

protected:
    TinyRedisProc*      m_redis;
};

#endif

