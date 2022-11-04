#include <pthread.h>
#include "common/log.h"
#include "rehasher.h"

int ReHasher::init(TinyRedisDB* db)
{
    m_redisDB = db;
    m_stop = 0;

    return 0;
}

void ReHasher::run()
{
    while (!m_stop)
    {
        for (int i = 0; i < m_redisDB->dbnum; i++)
        {
            /*
            long long size, used;
            size = dictSlots(m_redisDB->db[i].d);
            used = dictSize(m_redisDB->db[i].d);
            if (used)
                DLOG("db: %d, size: %d, used: %d, rate: %d", i, size, used, size ? used*100/size: 0);
            */

            if (!dictIsRehashing(m_redisDB->db[i].d) && htNeedsResize(m_redisDB->db[i].d))
            {
                pthread_rwlock_wrlock(&m_redisDB->db[i].rwlock);
                dictResize(m_redisDB->db[i].d);
                pthread_rwlock_unlock(&m_redisDB->db[i].rwlock);
            }

            if (dictIsRehashing(m_redisDB->db[i].d))
            {
                DLOG("do rehash for db: %d", i);
                pthread_rwlock_wrlock(&m_redisDB->db[i].rwlock);
                dictRehashMilliseconds(m_redisDB->db[i].d, 1);
                pthread_rwlock_unlock(&m_redisDB->db[i].rwlock);
            }

            //hashmap 的rehash 在hashmap的写操作中完成
        }

        //休眠200ms
        usleep(2000000);
    }
}

void ReHasher::stop()
{
    m_stop = 1;
}


