#include <assert.h>

#include "common/log.h"
#include "worker.h"

int Worker::init(TinyRedisDB* db)
{
    if (!db)
        return -1;

    int fd[2] = {-1, -1};
    if (pipe(fd) != 0)
    {
        ELOG("pipe failed! errno: %d, error: %s", errno, strerror(errno));
        return -2;
    }

    anetNonBlock(NULL, fd[0]);
    anetNonBlock(NULL, fd[1]);

    m_redis = CreateTinyRedisProc(db, fd[0], fd[1]);
    if (!m_redis)
    {
        ELOG("CreateTinyRedisProc failed!");
        return -3;
    }

    return 0;
}

void Worker::run()
{
    assert(m_redis);

    aeMain(m_redis->el);
}

void Worker::stop()
{
    if (m_redis)
        aeStop(m_redis->el);
}
