#ifndef __REHASH_H__
#define __REHASH_H__

#include "util/thread.h"
#include "tiny-redis/server.h"

class ReHasher : public ThreadBase 
{
public:
    int init(TinyRedisDB* db);

    virtual void run();

    virtual void stop();

protected:
    TinyRedisDB*    m_redisDB;

    int             m_stop;
};

#endif
