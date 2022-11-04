#include "common/log.h"
#include "mongo_cli.h"
#include "util/util.h"

bool MongoCli::m_inited = false;
TLock MongoCli::m_lockInited;

MongoCli::MongoCli(const string& url, const string& db, const string& collection)
{
    m_url = url;
    m_dbName = db;
    m_collectionName = collection;

    m_client = NULL;
    m_db = NULL;
    m_collection = NULL;

    if (!m_inited)
    {
        Guard g(m_lockInited);
        if (!m_inited)
        {
            mongoc_init();
            mongoc_log_set_handler(MongoCli::MongoCLog, NULL);
            m_inited = true;
        }
    }
}

MongoCli::~MongoCli()
{
    if (m_collection)
    {
        mongoc_collection_destroy(m_collection);
        m_collection = NULL;
    }

    if (m_db)
    {
        mongoc_database_destroy(m_db);
        m_db = NULL;
    }

    if (m_client)
    {
        mongoc_client_destroy(m_client);
        m_client = NULL;
    }
}

int MongoCli::init()
{
    if (!m_client)
    {
        m_client = mongoc_client_new(m_url.c_str());
        if (!m_client)
        {
            FDLOG("error") << "mongoc_client_new failed! url: " << m_url 
                << ", db: " << m_dbName << ", collection: " << m_collectionName << endl;
            return -1;
        }
    }

    if (!m_db)
    {
        m_db = mongoc_client_get_database(m_client, m_dbName.c_str());
        if (!m_db)
        {
            FDLOG("error") << "mongoc_client_get_database failed! url: " << m_url 
                << ", db: " << m_dbName << ", collection: " << m_collectionName << endl;
            return -2;
        }
    }

    if (!m_collection)
    {
        m_collection = mongoc_client_get_collection(m_client, m_dbName.c_str(), m_collectionName.c_str());
        if (!m_collection)
        {
            FDLOG("error") << "mongoc_client_get_collection failed! url: " << m_url 
                << ", db: " << m_dbName << ", collection: " << m_collectionName << endl;
            return -3;
        }
    }

    return 0;
}

void MongoCli::MongoCLog(mongoc_log_level_t level, const char* domain, const char* message, void* pdata)
{
    FDLOG("mongoc") << "domain: " << domain << ", msg: " << message << endl;
    if (level < MONGOC_LOG_LEVEL_WARNING)
    {
        FDLOG("error") << "in mongoc domain: " << domain  << ", msg: " << message << endl;
    }
}

int MongoCli::query(const string& condition, const string& opt, vector<string>& results)
{
    int ret = 0;
    bson_error_t error;
    bson_t* bcond = NULL;
    bson_t* bopt = NULL;
    
    if (!m_collection)
    {
        init();
        if (!m_collection)
        {
            FDLOG("error") << "MongoCli::query collection is NULL!" << endl;
            return -1;
        }
    }

    if (!condition.empty())
    {
        bcond = bson_new_from_json((const uint8_t*)condition.c_str(), -1, &error);
        if (!bcond)
        {
            FDLOG("error") << "MongoCli::query bson_new_from_json failed! cond: " << condition
                        << ", opt: " << opt << ", err: " << error.message << endl;
            return -2;
        }
    }

    if (!opt.empty())
    {
        bopt = bson_new_from_json((const uint8_t*)opt.c_str(), -1, &error);
        if (!bopt)
        {
            FDLOG("error") << "MongoCli::query bson_new_from_json failed! cond: " << condition
                        << ", opt: " << opt << ", err: " << error.message << endl;
            if (bcond)
                bson_destroy(bcond);
            return -2;
        }
    }

    bool have = false;
    const bson_t* doc = NULL;
    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(m_collection, bcond, bopt, NULL);
    while (mongoc_cursor_next(cursor, &doc))
    {
        char* str = bson_as_json(doc, NULL);
        if (str)
        {
            results.push_back(str);
            have = true;
            bson_free(str);
            ret++;
        }
    }

    if (mongoc_cursor_error(cursor, &error)) 
    {
        FDLOG("error") << " MongoCli::query error! cond: " << condition << ", opt: " << opt << ", have: " << have << ", err: " << error.message << endl;
        if (!have)
            ret = -3;
    }

    if (bcond)
        bson_destroy(bcond);
    if (bopt)
        bson_destroy(bopt);
    if (cursor)
        mongoc_cursor_destroy(cursor);

    return ret;
}

int MongoCli::update(const string& condition, const string& value)
{
    bson_error_t error;
    bson_t* bcond = NULL;
    
    if (!m_collection)
    {
        init();
        if (!m_collection)
        {
            FDLOG("error") << "MongoCli::update collection is NULL!" << endl;
            return -1;
        }
    }

    if (!condition.empty())
    {
        bcond = bson_new_from_json((const uint8_t*)condition.c_str(), -1, &error);
        if (!bcond)
        {
            FDLOG("error") << "MongoCli::update condition bson_new_from_json failed! json: " << condition
                        << ", err: " << error.message << endl;
            return -2;
        }
    }

    bson_t* bval = bson_new_from_json((const uint8_t*)value.c_str(), -1, &error);
    if (!bval)
    {
        FDLOG("error") << "MongoCli::update value bason_new_from_json failed! json: " << value
                        << ", err: " << error.message << endl;
        return -3;
    }

    bson_t* bsetval = BCON_NEW("$set", bval);

    bool bret = mongoc_collection_update(m_collection, MONGOC_UPDATE_NONE, bcond, bsetval, NULL, &error);
    if (!bret)
    {
        FDLOG("error") << "MongoCli::update mongoc_collection_update failed!, cond: " << condition
                        << ", val: " << value << ", err: " << error.message << endl;
        return -4;
    }

    if (bcond)
        bson_destroy(bcond);
    if (bval)
        bson_destroy(bval);
    if (bsetval)
        bson_destroy(bsetval);

    return 0;
}


int MongoCli::create(const string& uid, int version, const string& app, 
            const map<string, CategoryInfo>& categorys, const map<string, TagInfo>& tags)
{
    map<string, pair<int, double> > categorysStat;
    map<string, pair<int, double> > tagsStat;
    
    if (!m_collection)
    {
        init();
        if (!m_collection)
        {
            FDLOG("error") << "MongoCli::create collection is NULL!" << endl;
            return -1;
        }
    }
    int64_t now = (int64_t)(Util::us() / 1000000);
    
    bson_t* doc = BCON_NEW("_id", BCON_UTF8(uid.c_str()),
                            "v", BCON_INT32(version),
                            "app", BCON_UTF8(app.c_str()),
                            "ts", BCON_INT64(now));
    if (!doc)
    {
        FDLOG("error") << "MongoCli::create BCON_NEW failed! uid: " << uid 
                        << ", version: " << version << ", app: " << app
                        << ", categorys: " << categorys.size() << ", tags: " << tags.size() << endl;
       return -2; 
    }

    bson_t cs;
    BSON_APPEND_DOCUMENT_BEGIN(doc, "category", &cs);
    for (map<string, CategoryInfo>::const_iterator it = categorys.begin(); it != categorys.end(); ++it)
    {
        bson_t cv;
        BSON_APPEND_DOCUMENT_BEGIN(&cs, it->first.c_str(), &cv);
        BSON_APPEND_UTF8(&cv, "config", it->second.config.c_str());
        BSON_APPEND_INT64(&cv, "ts", now);
        
        bson_t w;
        BSON_APPEND_ARRAY_BEGIN(&cv, "w", &w);
        int index = 0;
        for (vector<WeightedInfo>::const_iterator itw = it->second.weighteds.begin(); itw != it->second.weighteds.end(); ++itw)
        {
            string strIndex = Util::tostr(index++);
            bson_t ow;
            BSON_APPEND_DOCUMENT_BEGIN(&w, strIndex.c_str(), &ow);
            BSON_APPEND_UTF8(&ow, "k", itw->key.c_str());
            BSON_APPEND_DOUBLE(&ow, "v", itw->weighted);
            bson_append_document_end(&w, &ow);

            categorysStat[it->first].first++;
            categorysStat[it->first].second += itw->weighted;
        }
        bson_append_array_end(&cv, &w);

        bson_append_document_end(&cs, &cv);
    }
    bson_append_document_end(doc, &cs);

    bson_t cst;
    BSON_APPEND_DOCUMENT_BEGIN(doc, "category_stat", &cst);
    for (map<string, pair<int, double> >::iterator it = categorysStat.begin(); it != categorysStat.end(); ++it)
    {
        bson_t st;
        BSON_APPEND_DOCUMENT_BEGIN(&cst, it->first.c_str(), &st);
        BSON_APPEND_INT32(&st, "num", it->second.first);
        BSON_APPEND_DOUBLE(&st, "sum", it->second.second);
        bson_append_document_end(&cst, &st);
    }
    bson_append_document_end(doc, &cst);
    
    bson_t ts;
    BSON_APPEND_DOCUMENT_BEGIN(doc, "tag", &ts);
    for (map<string, TagInfo>::const_iterator it = tags.begin(); it != tags.end(); ++it)
    {
        bson_t tv;
        BSON_APPEND_DOCUMENT_BEGIN(&ts, it->first.c_str(), &tv);
        BSON_APPEND_UTF8(&tv, "config", it->second.config.c_str());
        BSON_APPEND_INT64(&tv, "ts", now);
        
        bson_t w;
        BSON_APPEND_ARRAY_BEGIN(&tv, "w", &w);
        int index = 0;
        for (vector<WeightedInfo>::const_iterator itw = it->second.weighteds.begin(); itw != it->second.weighteds.end(); ++itw)
        {
            string strIndex = Util::tostr(index++);
            bson_t ow;
            BSON_APPEND_DOCUMENT_BEGIN(&w, strIndex.c_str(), &ow);
            BSON_APPEND_UTF8(&ow, "k", itw->key.c_str());
            BSON_APPEND_DOUBLE(&ow, "v", itw->weighted);
            bson_append_document_end(&w, &ow);

            tagsStat[it->first].first++;
            tagsStat[it->first].second += itw->weighted;
        }
        bson_append_array_end(&tv, &w);

        bson_append_document_end(&ts, &tv);
    }
    bson_append_document_end(doc, &ts);

    bson_t tss;
    BSON_APPEND_DOCUMENT_BEGIN(doc, "tag_stat", &tss);
    for (map<string, pair<int, double> >::iterator it = tagsStat.begin(); it != tagsStat.end(); ++it)
    {
        bson_t st;
        BSON_APPEND_DOCUMENT_BEGIN(&tss, it->first.c_str(), &st);
        BSON_APPEND_INT32(&st, "num", it->second.first);
        BSON_APPEND_DOUBLE(&st, "sum", it->second.second);
        bson_append_document_end(&tss, &st);
    }
    bson_append_document_end(doc, &tss);

    
    int ret = 0;
    bson_error_t error;
    bool bret = mongoc_collection_insert(m_collection, MONGOC_INSERT_NONE, doc, NULL, &error);
    if (!bret)
    {
        char* str = bson_as_json(doc, NULL);
        if (str && strlen(str) > 32) str[32] = '\0';
        FDLOG("error") << "MongoCli::create mongoc_collection_insert failed! json: " << (str ? str : "") 
                        << ", errdomain: " << error.domain << ", errno: " << error.code
                        << ", error: " << error.message << endl;
        ret = -3;
        if (error.code == MONGOC_ERROR_DUPLICATE_KEY)
            ret = 1;
        if (str)
            bson_free(str);
    }

    
    bson_destroy(doc);

    return ret;
}

int MongoCli::update(const string& uid, int version, const string& app, 
            const map<string, CategoryInfo>& categorys, const map<string, TagInfo>& tags)
{
    map<string, pair<int, double> > categorysStat;
    map<string, pair<int, double> > tagsStat;
    
    if (!m_collection)
    {
        init();
        if (!m_collection)
        {
            FDLOG("error") << "MongoCli::update collection is NULL!" << endl;
            return -1;
        }
    }

    int64_t now = (int64_t)(Util::us() / 1000000);
    bson_t qb;
    bson_t ubcmd, ub;

    bson_init(&qb);
    bson_init(&ubcmd);

    BSON_APPEND_UTF8(&qb, "_id", uid.c_str());
    
    BSON_APPEND_DOCUMENT_BEGIN(&ubcmd, "$set", &ub);

    if (version > 0)
        BSON_APPEND_INT32(&ub, "v", version);
    if (!app.empty())
        BSON_APPEND_UTF8(&ub, "app", app.c_str());
    BSON_APPEND_INT64(&ub, "ts", now);

    if (categorys.size() > 0)
    {
        for (map<string, CategoryInfo>::const_iterator it = categorys.begin(); it != categorys.end(); ++it)
        {
            bson_t cv;
            string scheme = "category." + it->first;
            BSON_APPEND_DOCUMENT_BEGIN(&ub, scheme.c_str(), &cv);
            BSON_APPEND_UTF8(&cv, "config", it->second.config.c_str());
            BSON_APPEND_INT64(&cv, "ts", now);

            bson_t w;
            BSON_APPEND_ARRAY_BEGIN(&cv, "w", &w);
            int index = 0;
            for (vector<WeightedInfo>::const_iterator itw = it->second.weighteds.begin(); itw != it->second.weighteds.end(); ++itw)
            {
                string strIndex = Util::tostr(index++);
                bson_t ow;
                BSON_APPEND_DOCUMENT_BEGIN(&w, strIndex.c_str(), &ow);
                BSON_APPEND_UTF8(&ow, "k", itw->key.c_str());
                BSON_APPEND_DOUBLE(&ow, "v", itw->weighted);
                bson_append_document_end(&w, &ow);

                categorysStat[it->first].first++;
                categorysStat[it->first].second += itw->weighted;
            }
            bson_append_array_end(&cv, &w);

            bson_append_document_end(&ub, &cv);
        }

        for (map<string, pair<int, double> >::iterator it = categorysStat.begin(); it != categorysStat.end(); ++it)
        {
            bson_t st;
            string scheme = "category_stat." + it->first;
            BSON_APPEND_DOCUMENT_BEGIN(&ub, scheme.c_str(), &st);
            BSON_APPEND_INT32(&st, "num", it->second.first);
            BSON_APPEND_DOUBLE(&st, "sum", it->second.second);
            bson_append_document_end(&ub, &st);
        }
    }

    if (tags.size() > 0)
    {
        for (map<string, TagInfo>::const_iterator it = tags.begin(); it != tags.end(); ++it)
        {
            bson_t tv;
            string scheme = "tag." + it->first;
            BSON_APPEND_DOCUMENT_BEGIN(&ub, scheme.c_str(), &tv);
            BSON_APPEND_UTF8(&tv, "config", it->second.config.c_str());
            BSON_APPEND_INT64(&tv, "ts", now);

            bson_t w;
            BSON_APPEND_ARRAY_BEGIN(&tv, "w", &w);
            int index = 0;
            for (vector<WeightedInfo>::const_iterator itw = it->second.weighteds.begin(); itw != it->second.weighteds.end(); ++itw)
            {
                string strIndex = Util::tostr(index++);
                bson_t ow;
                BSON_APPEND_DOCUMENT_BEGIN(&w, strIndex.c_str(), &ow);
                BSON_APPEND_UTF8(&ow, "k", itw->key.c_str());
                BSON_APPEND_DOUBLE(&ow, "v", itw->weighted);
                bson_append_document_end(&w, &ow);

                tagsStat[it->first].first++;
                tagsStat[it->first].second += itw->weighted;
            }
            bson_append_array_end(&tv, &w);

            bson_append_document_end(&ub, &tv);
        }

        for (map<string, pair<int, double> >::iterator it = tagsStat.begin(); it != tagsStat.end(); ++it)
        {
            bson_t st;
            string scheme = "tag_stat." + it->first;
            BSON_APPEND_DOCUMENT_BEGIN(&ub, scheme.c_str(), &st);
            BSON_APPEND_INT32(&st, "num", it->second.first);
            BSON_APPEND_DOUBLE(&st, "sum", it->second.second);
            bson_append_document_end(&ub, &st);
        }
    }

    bson_append_document_end(&ubcmd, &ub);

    int ret = 0;
    bson_error_t error;
    bool bret = mongoc_collection_update(m_collection, MONGOC_UPDATE_UPSERT, &qb, &ubcmd, NULL, &error);
    if (!bret)
    {
        char* str = bson_as_json(&ubcmd, NULL);
        if (str && strlen(str) > 32) str[32] = '\0';
        FDLOG("error") << "MongoCli::udpate mongoc_collection_update failed! json: " << (str ? str : "") 
                        << ", errdomain: " << error.domain << ", errno: " << error.code
                        << ", error: " << error.message << endl;
        if (str)
            bson_free(str);
        ret = -3;
    }
    
    bson_destroy(&qb);
    bson_destroy(&ubcmd);

    return ret;
}

int MongoCli::query(const string& uid, int version, string& app,
                bool allCategorys, map<string, CategoryInfo>& categorys,
                bool allTags, map<string, TagInfo>& tags)
{
    int ret = 0;
    if (!m_collection)
    {
        init();
        if (!m_collection)
        {
            FDLOG("error") << "MongoCli::query uid: " << uid << ", app: " << app << ", collection is NULL!" << endl;
            return -1;
        }
    }
    
    bson_error_t error;
    bson_t qb;
    bson_init(&qb);
    BSON_APPEND_UTF8(&qb, "_id", uid.c_str());
    
    bson_t optb, fb;
    bson_init(&optb);
    BSON_APPEND_INT64(&optb, "limit", 1);

    BSON_APPEND_DOCUMENT_BEGIN(&optb, "projection", &fb);

    BSON_APPEND_BOOL(&fb, "_id", false);
    BSON_APPEND_BOOL(&fb, "v", true);
    BSON_APPEND_BOOL(&fb, "app", true);

    if (allCategorys)
    {
        BSON_APPEND_INT32(&fb, "category", true);
    }
    else
    {
        for (map<string, CategoryInfo>::iterator it = categorys.begin(); it != categorys.end(); ++it)
        {
            string scheme = "category." + it->first;
            BSON_APPEND_INT32(&fb, scheme.c_str(), true);
        }
    }

    if (allTags)
    {
        BSON_APPEND_INT32(&fb, "tag", true);
    }
    else
    {
        for (map<string, TagInfo>::iterator it = tags.begin(); it != tags.end(); ++it)
        {
            string scheme = "tag." + it->first;
            BSON_APPEND_INT32(&fb, scheme.c_str(), true);
        }
    }

    bson_append_document_end(&optb, &fb);

    bool have = false;
    const bson_t* doc = NULL;
    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(m_collection, &qb, &optb, NULL);
    while (mongoc_cursor_next(cursor, &doc))
    {
        if (!doc)
        {
            continue;
        }

        ParseODoc(uid, *doc, version, app, categorys, tags);
        ret = 1;
        have = true;
    }
    
    if (mongoc_cursor_error(cursor, &error)) 
    {
        FDLOG("error") << " MongoCli::query error! uid: " << uid << ", app: " << app << ", have: " << have << ", err: " << error.message << endl;
        if (!have)
            ret = -2;
    }

    if (cursor)
        mongoc_cursor_destroy(cursor);

    bson_destroy(&qb);
    bson_destroy(&optb);

    return ret;
}

/*
 *  {
 *      "v" : 1,
 *
 *      "_id" : "123xx00",
 *      "app" : "coolpad",
 *
 *      "category" :    { 
 *                          "v1" :  {
 *                                      "config" : "test", 
 *                                      "ts" : 123456, 
 *                                      "w" : [{"k" : "8", "v" : 0.1}, {"k" : "2", "v" : 0.33}]
 *                                  },
 *                          "v2" :  {
 *                                      "config" : "t3",
 *                                      "ts" : 3333333,
 *                                      "w" : []
 *                                  }
 *                      },
 *
 *      "tag" :         {
 *                          "v1" :  {
 *                                      "config" : "test",
 *                                      "ts" : 33333,
 *                                      "w" : [{"k" : "haha", "v" : 0.3}, {"k" : "test", "v" : 1.222}]
 *                                  },
 *                          "v11" : {
 *                                      "config" : "test11",
 *                                      "ts" : 1111133333,
 *                                      "w" : [{"k" : "haha2", "v" : 0.3}, {"k" : "te", "v" : 1.222}]
 *                                  }
 *                      },
 *
 *      "category_stat":{
 *                          "v1" : {"num" : 2, "sum" : 0.43},
 *                          "v2" : {"num" : 0, "sum" : 0.0}
 *                      },
 *
 *      "tag_stat" :    {
 *                          "v1" : {"num" : 2, "sum" : 1.522},
 *                          "v11" :{"num" : 2, "sum" : 1.522}
 *                      }
 *  }
 *
 */
int MongoCli::ParseODoc(const string& uid, const bson_t& b, int& version, string& app, 
                        map<string, CategoryInfo>& categorys, map<string, TagInfo>& tags)
{
    bson_iter_t iter;
    if (!bson_iter_init(&iter, &b))
    {
        FDLOG("error") << "MongoCli::ParseODoc uid: " << uid << ", bson_iter_init failed!" << endl;
        return -1; 
    }

    while (bson_iter_next(&iter))
    {
        const char* key = bson_iter_key(&iter);
        if (0 == strncmp(key, "v", strlen("v")) && key[strlen("v")] == '\0')
        {
            version = (int)bson_iter_as_int64(&iter);
        }
        else if (0 == strncmp(key, "app", strlen("app")) && key[strlen("app")] == '\0')
        {
            uint32_t len;
            const char* buf = bson_iter_utf8(&iter, &len);
            app.assign(buf, len);
        }
        else if (0 == strncmp(key, "category", strlen("category")) && key[strlen("category")] == '\0')
        {
            if (bson_iter_type(&iter) != BSON_TYPE_DOCUMENT)
            {
                FDLOG("error") << "MonogCli::ParseODoc uid: " << uid << ", expect category type: " << BSON_TYPE_DOCUMENT 
                    << ", but: " << bson_iter_type(&iter) << endl;
                continue;
            }

            const uint8_t* buf = NULL;
            uint32_t len = 0;
            bson_iter_document(&iter, &len, &buf);

            bson_t cg;
            bson_init(&cg);
            bson_init_static(&cg, buf, len);
            ParseCategory(uid, cg, categorys);
        }
        else if (0 == strncmp(key, "tag", strlen("tag")) && key[strlen("tag")] == '\0')
        {
            if (bson_iter_type(&iter) != BSON_TYPE_DOCUMENT)
            {
                FDLOG("error") << "MonogCli::ParseODoc uid: " << uid << ", expect tag type: " << BSON_TYPE_DOCUMENT 
                    << ", but: " << bson_iter_type(&iter) << endl;
                continue;
            }

            const uint8_t* buf = NULL;
            uint32_t len = 0;
            bson_iter_document(&iter, &len, &buf);

            bson_t tg;
            bson_init(&tg);
            bson_init_static(&tg, buf, len);
            ParseTag(uid, tg, tags);
        }
    }

    return 0;
}

/*
 *  { 
 *      "v1" :  {
 *                  "config" : "test", 
 *                  "ts" : 123456, 
 *                  "w" : [{"k" : "8", "v" : 0.1}, {"k" : "2", "v" : 0.33}]
 *              },
 *      "v2" :  {
 *                  "config" : "t3",
 *                  "ts" : 3333333,
 *                  "w" : []
 *              }
 *  }
 *
 */
int MongoCli::ParseCategory(const string& uid, bson_t& b, map<string, CategoryInfo>& categorys)
{
    bson_iter_t iter;
    if (!bson_iter_init(&iter, &b))
    {
        FDLOG("error") << "MongoCli::ParseCategory uid: " << uid << ", bson_iter_init for category failed!" << endl;
        return -1; 
    }

    while (bson_iter_next(&iter))
    {
        const char* vkey = bson_iter_key(&iter);

        if (bson_iter_type(&iter) != BSON_TYPE_DOCUMENT)
        {
            FDLOG("error") << "MonogCli::ParseCategory uid: " << uid << ", expect category." << vkey << " type: " << BSON_TYPE_DOCUMENT 
                << ", but: " << bson_iter_type(&iter) << endl;
            continue;
        }

        const uint8_t* vbuf = NULL;
        uint32_t vlen = 0;
        bson_iter_document(&iter, &vlen, &vbuf);

        bson_t vcg;
        bson_init(&vcg);
        bson_init_static(&vcg, vbuf, vlen);

        ParseOCategory(uid, vkey, vcg, categorys[vkey]);      
    }

    return 0;
}

/*
 *  { 
 *      "v1" :  {
 *                  "config" : "test", 
 *                  "ts" : 123456, 
 *                  "w" : [{"k" : "haha", "v" : 0.3}, {"k" : "test", "v" : 1.222}]
 *              },
 *      "v2" :  {
 *                  "config" : "t3",
 *                  "ts" : 3333333,
 *                  "w" : []
 *              }
 *  }
 *
 */
int MongoCli::ParseTag(const string& uid, bson_t& b, map<string, TagInfo>& tags)
{
    bson_iter_t iter;
    if (!bson_iter_init(&iter, &b))
    {
        FDLOG("error") << "MongoCli::ParseTag uid: " << uid << ", bson_iter_init for category failed!" << endl;
        return -1; 
    }

    while (bson_iter_next(&iter))
    {
        const char* vkey = bson_iter_key(&iter);

        if (bson_iter_type(&iter) != BSON_TYPE_DOCUMENT)
        {
            FDLOG("error") << "MonogCli::ParseTag uid: " << uid << ", expect category." << vkey << " type: " << BSON_TYPE_DOCUMENT 
                << ", but: " << bson_iter_type(&iter) << endl;
            continue;
        }

        const uint8_t* vbuf = NULL;
        uint32_t vlen = 0;
        bson_iter_document(&iter, &vlen, &vbuf);

        bson_t vtg;
        bson_init(&vtg);
        bson_init_static(&vtg, vbuf, vlen);

        ParseOTag(uid, vkey, vtg, tags[vkey]);      
    }

    return 0;
}

/*
 *  {
 *      "config" : "t3",
 *      "ts" : 3333333,
 *      "w" : [{"k" : "8", "v" : 0.1}, {"k" : "2", "v" : 0.33}]
 *  }
 *
 */
int MongoCli::ParseOCategory(const string& uid, const string& v, bson_t& b, CategoryInfo& category)
{
    bson_iter_t iter;
    if (!bson_iter_init(&iter, &b))
    {
        FDLOG("error") << "MongoCli::ParseOCategory uid: " << uid << ", bson_iter_init for category." << v << " failed!" << endl;
        return -1;
    }

    while (bson_iter_next(&iter))
    {
        const char* cgkey = bson_iter_key(&iter);
        if (0 == strncmp(cgkey, "config", strlen("config")) && cgkey[strlen("config")] == '\0')
        {
            uint32_t len;
            const char* buf = bson_iter_utf8(&iter, &len);
            category.config.assign(buf, len);
        }
        else if (0 == strncmp(cgkey, "ts", strlen("ts")) && cgkey[strlen("ts")] == '\0')
        {
            category.ts = (time_t)bson_iter_as_int64(&iter);
        }
        else if (0 == strncmp(cgkey, "w", strlen("w")) && cgkey[strlen("w")] == '\0')
        {
            if (bson_iter_type(&iter) != BSON_TYPE_ARRAY)
            {
                FDLOG("error") << "MonogCli::ParseOCategory uid: " << uid << ", expect category." << cgkey << ".w type: " 
                    << BSON_TYPE_ARRAY << ", but: " << bson_iter_type(&iter) << endl;
                continue;
            }

            const uint8_t* wbuf = NULL;
            uint32_t wlen = 0;
            bson_iter_array(&iter, &wlen, &wbuf);

            bson_t wcg;
            bson_init(&wcg);
            bson_init_static(&wcg, wbuf, wlen);

            bson_iter_t itwcg;
            if (!bson_iter_init(&itwcg, &wcg))
            {
                FDLOG("error") << "MongoCli::ParseOCategory uid: " << uid << ", bson_iter_init for category." << cgkey << ".w failed!" << endl;
                continue;
            }

            while (bson_iter_next(&itwcg))
            {
                const char* index = bson_iter_key(&itwcg);
                if (bson_iter_type(&itwcg) != BSON_TYPE_DOCUMENT)
                {
                    FDLOG("error") << "MonogCli::ParseOCategory uid: " << uid << ", expect category." << cgkey << ".w." << index << " type: " 
                            << BSON_TYPE_DOCUMENT << ", but: " << bson_iter_type(&iter) << endl;
                    continue;
                }

                const uint8_t* kvbuf = NULL;
                uint32_t kvlen = 0;
                bson_iter_document(&itwcg, &kvlen, &kvbuf);

                bson_t kvcg;
                bson_init(&kvcg);
                bson_init_static(&kvcg, kvbuf, kvlen);

                bson_iter_t itkv;
                if (!bson_iter_init(&itkv, &kvcg))
                {
                    FDLOG("error") << "MongoCli::ParseOCategory uid: " << uid << ", bson_iter_init for category." << cgkey << ".w" << index << " failed!" << endl;
                    continue;
                }

                string strKey;
                double dVal = 0.0;
                while (bson_iter_next(&itkv))
                {
                    const char* kvkey = bson_iter_key(&itkv);
                    if (0 == strncmp(kvkey, "k", strlen("k") && kvkey[strlen("k")] == '\0'))
                    {
                        uint32_t len;
                        const char* buf = bson_iter_utf8(&itkv, &len);
                        strKey.assign(buf, len);
                    }
                    else if (0 == strncmp(kvkey, "v", strlen("v")) && kvkey[strlen("v")] == '\0')
                    {
                        dVal = bson_iter_double(&itkv);
                    }
                }
                if (strKey.empty())
                {
                    FDLOG("error") << "MongoCli::ParseOCategory uid: " << uid << ", invalid w" << endl;
                }
                else
                {
                    category.weighteds.push_back(WeightedInfo(strKey, dVal));
                }
            }
        }
    }

    return 0;
}

/*
 *  {
 *      "config" : "t3",
 *      "ts" : 3333333,
 *      "w" : [{"k" : "haha", "v" : 0.3}, {"k" : "test", "v" : 1.222}]
 *  }
 *
 */
int MongoCli::ParseOTag(const string& uid, const string& v, bson_t& b, TagInfo& tag)
{
    bson_iter_t iter;
    if (!bson_iter_init(&iter, &b))
    {
        FDLOG("error") << "MongoCli::ParseOTag uid: " << uid << ", bson_iter_init for tag." << v << " failed!" << endl;
        return -1;
    }

    while (bson_iter_next(&iter))
    {
        const char* tgkey = bson_iter_key(&iter);
        if (0 == strncmp(tgkey, "config", strlen("config")) && tgkey[strlen("config")] == '\0')
        {
            uint32_t len;
            const char* buf = bson_iter_utf8(&iter, &len);
            tag.config.assign(buf, len);
        }
        else if (0 == strncmp(tgkey, "ts", strlen("ts")) && tgkey[strlen("ts")] == '\0')
        {
            tag.ts = (time_t)bson_iter_as_int64(&iter);
        }
        else if (0 == strncmp(tgkey, "w", strlen("w")) && tgkey[strlen("w")] == '\0')
        {
            if (bson_iter_type(&iter) != BSON_TYPE_ARRAY)
            {
                FDLOG("error") << "MonogCli::ParseOTag uid: " << uid << ", expect tag." << tgkey << ".w type: " 
                    << BSON_TYPE_ARRAY << ", but: " << bson_iter_type(&iter) << endl;
                continue;
            }

            const uint8_t* wbuf = NULL;
            uint32_t wlen = 0;
            bson_iter_array(&iter, &wlen, &wbuf);

            bson_t wtg;
            bson_init(&wtg);
            bson_init_static(&wtg, wbuf, wlen);

            bson_iter_t itwtg;
            if (!bson_iter_init(&itwtg, &wtg))
            {
                FDLOG("error") << "MongoCli::ParseOTag uid: " << uid << ", bson_iter_init for tag." << tgkey << ".w failed!" << endl;
                continue;
            }

            while (bson_iter_next(&itwtg))
            {
                const char* index = bson_iter_key(&itwtg);
                if (bson_iter_type(&itwtg) != BSON_TYPE_DOCUMENT)
                {
                    FDLOG("error") << "MonogCli::ParseOTag uid: " << uid << ", expect tag." << tgkey << ".w." << index << " type: " 
                            << BSON_TYPE_DOCUMENT << ", but: " << bson_iter_type(&iter) << endl;
                    continue;
                }

                const uint8_t* kvbuf = NULL;
                uint32_t kvlen = 0;
                bson_iter_document(&itwtg, &kvlen, &kvbuf);

                bson_t kvtg;
                bson_init(&kvtg);
                bson_init_static(&kvtg, kvbuf, kvlen);

                bson_iter_t itkv;
                if (!bson_iter_init(&itkv, &kvtg))
                {
                    FDLOG("error") << "MongoCli::ParseOTag uid: " << uid << ", bson_iter_init for tag." << tgkey << ".w" << index << " failed!" << endl;
                    continue;
                }

                string strKey;
                double dVal = 0.0;
                while (bson_iter_next(&itkv))
                {
                    const char* kvkey = bson_iter_key(&itkv);
                    if (0 == strncmp(kvkey, "k", strlen("k") && kvkey[strlen("k")] == '\0'))
                    {
                        uint32_t len;
                        const char* buf = bson_iter_utf8(&itkv, &len);
                        strKey.assign(buf, len);
                    }
                    else if (0 == strncmp(kvkey, "v", strlen("v")) && kvkey[strlen("v")] == '\0')
                    {
                        dVal = bson_iter_double(&itkv);
                    }
                }
                if (strKey.empty())
                {
                    FDLOG("error") << "MongoCli::ParseOTag uid: " << uid << ", invalid w" << endl;
                }
                else
                {
                    tag.weighteds.push_back(WeightedInfo(strKey, dVal));
                }
            }
        }
    }

    return 0;
}
