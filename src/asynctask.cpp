#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include "asynctask.h"


#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"

#include "util/inv_coredis.h"
#include "common/log.h"

std::vector<ASyncTask*> ASyncTask::m_tasks;
std::bitset<0x10000>    ASyncTask::m_filter;

ASyncTask::~ASyncTask()
{
    delete m_queue;
    delete m_mongoCli;
    delete m_redis;
}

int ASyncTask::init(int queSize, 
        const std::string& url, const std::string& db, const std::string& collection,
        const std::string& redisIP, int redisPort, int redisTimeout)
{
    m_stop = 0;

    m_queue = new RingQue<MissTask>(queSize);
    m_lockQue = PTHREAD_MUTEX_INITIALIZER;
    m_condQue = PTHREAD_COND_INITIALIZER;

    m_mongoCli = new MongoCli(url, db, collection);
    int ret = m_mongoCli->init();

    m_redis = new inv::INV_CoRedis;
    m_redis->init(redisIP, redisPort, redisTimeout);

    return ret;
}

void ASyncTask::ExecMongoTask(const std::string& key)
{
    // [type&&uid&&version]
    std::vector<std::string> eles;
    Util::separate(key, "&&", eles);
    if (eles.size() < 2)
    {
        ELOG("ASyncTask::ExecMongoTask Unknow key: %s", key.c_str());
        return ;
    }

    #define DATA_TYPE_CATEGORY 1
    #define DATA_TYPE_TAG 2
    #define DATA_TYPE_CATEGORY_STAT 3
    #define DATA_TYPE_TAG_STAT 4

    int dataType = -1;
    if (eles[0] == "category")
        dataType = DATA_TYPE_CATEGORY;
    else if (eles[0] == "tag")
        dataType = DATA_TYPE_TAG;
    else if (eles[0] == "category_stat")
        dataType = DATA_TYPE_CATEGORY_STAT;
    else if (eles[0] == "tag_stat")
        dataType = DATA_TYPE_TAG_STAT;
    else 
    {
        ELOG("ASyncTask::ExecMongoTask Unkonw dataType! key: %s", key.c_str());
        return ;
    }

    if (m_redis->exists(eles[1]) <= 0)
    {
        DLOG("ASyncTask::ExecMongoTask no uid! key: %s, uid: %s", key.c_str(), eles[1].c_str());
        return ;
    }

    int ret = 0;
    std::string result;
    std::string appInDB;
    if (dataType == DATA_TYPE_CATEGORY)
    {
        if (eles.size() != 3)
        {
            ELOG("ASyncTask::ExecMongoTask invalid key: %s", key.c_str());
            return ;
        }

        std::map<std::string, CategoryInfo> cgs;
        std::map<std::string, TagInfo> tgs;
        cgs[eles[2]] = CategoryInfo();
        ret = m_mongoCli->query(eles[1], 0, appInDB, false, cgs, false, tgs);
        if (ret < 0)
        {
            ELOG("ASyncTask::ExecMongoTask query failed! key: %s, ret: %d", key.c_str(), ret);
            return ;
        }

        CategoryInfo& cg = cgs[eles[2]];
        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
        writer.SetMaxDecimalPlaces(3);
        writer.StartObject();
        writer.Key("ts");
        writer.Int(cg.ts);
        writer.Key("weighted");
        writer.StartArray();
        for (vector<WeightedInfo>::iterator it = cg.weighteds.begin(); it != cg.weighteds.end(); ++it)
        {
            writer.StartObject();
            writer.Key("tag");
            writer.String(it->key.c_str());
            writer.Key("weight");
            writer.Double(it->weighted);
            writer.EndObject();
        }
        writer.EndArray();
        writer.EndObject();
        result = buf.GetString();
    }
    else if (dataType == DATA_TYPE_TAG)
    {
        if (eles.size() != 3)
        {
            ELOG("ASyncTask::ExecMongoTask invalid key: %s", key.c_str());
            return ;
        }

        std::map<std::string, CategoryInfo> cgs;
        std::map<std::string, TagInfo> tgs;
        tgs[eles[2]] = TagInfo();
        ret = m_mongoCli->query(eles[1], 0, appInDB, false, cgs, false, tgs);
        if (ret < 0)
        {
            ELOG("ASyncTask::ExecMongoTask query failed! key: %s, ret: %d", key.c_str(), ret);
            return ;
        }

        TagInfo& tg = tgs[eles[2]];
        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
        writer.SetMaxDecimalPlaces(3);
        writer.StartObject();
        writer.Key("ts");
        writer.Int(tg.ts);
        writer.Key("weighted");
        writer.StartArray();
        for (vector<WeightedInfo>::iterator it = tg.weighteds.begin(); it != tg.weighteds.end(); ++it)
        {
            writer.StartObject();
            writer.Key("tag");
            writer.String(it->key.c_str());
            writer.Key("weight");
            writer.Double(it->weighted);
            writer.EndObject();
        }
        writer.EndArray();
        writer.EndObject();
        result = buf.GetString();
    }
    else if (dataType == DATA_TYPE_CATEGORY_STAT)
    {
        if (eles.size() != 2)
        {
            ELOG("ASyncTask::ExecMongoTask invalid key: %s", key.c_str());
            return ;
        }

        std::string condition = "{\"_id\":\"" + eles[1] + "\"}";
        std::string opt = "{\"projection\":{\"_id\":0,\"tag_stat\":1,\"ts\":1}}";
        std::vector<std::string> vecRs;
        ret = m_mongoCli->query(condition, opt, vecRs);
        if (ret < 0)
        {
            ELOG("ASyncTask::ExecMongoTask query failed! key: %s, ret: %d", key.c_str(), ret);
            return ;
        }

        if (vecRs.size() == 0)
        {
            result = "{}";
        }
        else
        {
            rapidjson::Document d;
            d.Parse(vecRs[0].c_str());
            if (d.HasParseError() || !d.HasMember("ts") || !d["ts"].IsInt() || !d.HasMember("category_stat"))
            {
                ELOG("ASyncTask::ExecMongoTask invalid queried data! key: %s, data: %s", key.c_str(), vecRs[0].c_str());
                result = "{}";
            }
            else
            {
                rapidjson::Value& category_stat = d["category_stat"];
                d.AddMember("data", category_stat, d.GetAllocator());
                d.RemoveMember("category_stat");
                rapidjson::StringBuffer buf;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
                d.Accept(writer);
                result = buf.GetString();
            }
        }
    }
    else if (dataType == DATA_TYPE_TAG_STAT)
    {
        if (eles.size() != 2)
        {
            ELOG("ASyncTask::ExecMongoTask invalid key: %s", key.c_str());
            return ;
        }
        
        std::string condition = "{\"_id\":\"" + eles[1] + "\"}";
        std::string opt = "{\"projection\":{\"_id\":0,\"tag_stat\":1,\"ts\":1}}";
        std::vector<std::string> vecRs;
        ret = m_mongoCli->query(condition, opt, vecRs);
        if (ret < 0)
        {
            ELOG("ASyncTask::ExecMongoTask query failed! key: %s, ret: %d", key.c_str(), ret);
            return ;
        }

        if (vecRs.size() == 0)
        {
            result = "{}";
        }
        else
        {
            rapidjson::Document d;
            d.Parse(vecRs[0].c_str());
            if (d.HasParseError() || !d.HasMember("ts") || !d["ts"].IsInt() || !d.HasMember("tag_stat"))
            {
                ELOG("ASyncTask::ExecMongoTask invalid queried data! key: %s, data: %s", key.c_str(), vecRs[0].c_str());
                result = "{}";
            }
            else
            {
                rapidjson::Value& category_stat = d["tag_stat"];
                d.AddMember("data", category_stat, d.GetAllocator());
                d.RemoveMember("tag_stat");
                rapidjson::StringBuffer buf;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
                d.Accept(writer);
                result = buf.GetString();
            }
        }
    }
    else 
    {
        ELOG("ASyncTask::ExecMongoTask Unknow key: %s", key.c_str());
        return ;
    }

    unsigned int slot = keyHashSlot(key.c_str(), (int)key.size());
    if ((int)slot < g_redisDB->dbnum)
    {
        robj* objKey = createStringObject(key.c_str(), key.size());
        robj* objVal = createStringObject(result.c_str(), result.size());
        
        {
            pthread_rwlock_wrlock(&g_redisDB->db[slot].rwlock);
            setKey(&g_redisDB->db[slot], objKey, objVal, (7 * 24 * 60 * 60 * 1000));
            g_redisDB->db[slot].dirty++;
            pthread_rwlock_unlock(&g_redisDB->db[slot].rwlock);
        }

        decrRefCount(objKey);
        decrRefCount(objVal);
    }
}

void ASyncTask::run()
{
    struct timespec timeout;
    struct timeval now;
    pthread_mutex_lock(&m_lockQue);
    while (!m_stop)
    {
        //10ms 扫描一次
        gettimeofday(&now, NULL);
        timeout.tv_sec = now.tv_sec;
        timeout.tv_nsec = (now.tv_usec + (10 * 1000)) * 1000;
        pthread_cond_timedwait(&m_condQue, &m_lockQue, &timeout); 
        
        MissTask task;
        while (!m_stop && m_queue->Pop(task) >= 0) 
        {
            DLOG("To Deal ASync Task! key: %s", task.key.c_str());
            ExecMongoTask(task.key);

            uint16_t h = crc16(task.key.c_str(), task.key.size()) % m_filter.size();
            m_filter.reset(h);
        } 
    }
    pthread_mutex_unlock(&m_lockQue);
}

void ASyncTask::stop()
{
    m_stop = 1;
    pthread_cond_signal(&m_condQue);
}

int ASyncTask::Start(int n, int queSize, 
        const std::string& url, const std::string& db, const std::string& collection,
        const std::string& redisIP, int redisPort, int redisTimeout)
{
    for (int i = 0; i < n; i++)
    {
        ASyncTask* t = new ASyncTask;
        t->init(queSize, url, db, collection, redisIP, redisPort, redisTimeout);
        m_tasks.push_back(t);
    }

    for (int i = 0; i < (int)m_tasks.size(); i++)
        m_tasks[i]->start();

    return 0;
}

void ASyncTask::Stop()
{
    for (int i = 0; i < (int)m_tasks.size(); i++)
    {
        m_tasks[i]->stop();
    }

    for (int i = 0; i < (int)m_tasks.size(); i++)
    {
        pthread_join(m_tasks[i]->getid(), NULL);
        delete m_tasks[i];
    }

    m_tasks.clear();
}

int ASyncTask::PushTask(const char* key)
{
    MissTask task;
    
    if (m_tasks.size() == 0)
        return -101;
    
    uint16_t h = crc16(key, strlen(key)) % m_filter.size();
    if (m_filter.test(h))
    {
        DLOG("ASyncTask::PushTask A Task(%u) Is Running!", h);
        return -100;
    }
    m_filter.set(h);

    task.ms = Util::ms();
    task.key = key;

    int index = rand() % m_tasks.size();
    int minIndex = index;
    uint32_t minLen = (uint32_t)(-1);
    for (int i = 0; i < (int)m_tasks.size(); i++, index = (index + 1) % m_tasks.size())
    {
        if (m_tasks[index] && m_tasks[index]->m_queue->Len() < minLen)
        {
            minIndex = index;
            minLen = m_tasks[index]->m_queue->Len();
        }

        if (minLen == 0)
            break;
    }

    int ret = m_tasks[minIndex]->m_queue->Push(task);
    pthread_cond_signal(&m_tasks[minIndex]->m_condQue);
    if (ret < 0)
        m_filter.reset(h);
    DLOG("ASyncTask::PushTask id: %d, h: %u, key: %s, ret: %d", minIndex, h, key, ret);
    
    return ret;
}

