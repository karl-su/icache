#ifndef __MONGO_CLI_H__
#define __MONGO_CLI_H__

#include <libmongoc-1.0/mongoc.h>
#include <libbson-1.0/bson.h>
#include <libbson-1.0/bcon.h>

#include <string>
#include <vector>
#include <map>

#include "util/lock.h"

using namespace std;

/*********** SCHEME ********************/
typedef struct WeightedStat {
    int                     num;
    double                  sum;
} WeightedStat;

typedef struct WeightedInfo {
    string                  key;
    double                  weighted;

    WeightedInfo(const string& k, double w) : key(k), weighted(w) {}
    WeightedInfo() : key(""), weighted(0.0) {}

    bool operator < (const WeightedInfo& rs) const 
    { return this->weighted < rs.weighted; }
} WeightedInfo;

typedef struct CategoryInfo {
    string                  config;
    time_t                  ts;
    vector<WeightedInfo>    weighteds;

    CategoryInfo(const string& c, time_t t, const vector<WeightedInfo>& w) : config(c), ts(t), weighteds(w) {}
    CategoryInfo() : config(""), ts(0) {}
} CategoryInfo;

typedef struct TagInfo {
    string                  config;
    time_t                  ts;
    vector<WeightedInfo>    weighteds;

    TagInfo(const string& c, time_t t, const vector<WeightedInfo>& w) : config(c), ts(t), weighteds(w) {}
    TagInfo() : config(""), ts(0) {}
} TagInfo;
/**************************************/

class MongoCli {
public:
    MongoCli(const string& url, const string& db, const string& collection);

    virtual ~MongoCli();

    int init();

    static void MongoCLog(mongoc_log_level_t level, const char* domain, const char* message, void* pdata);

    int query(const string& condition, const string& opt, vector<string>& results);

    int update(const string& condition, const string& value);

    int create(const string& uid, int version, const string& app, 
                const map<string, CategoryInfo>& categorys, const map<string, TagInfo>& tags);

    int update(const string& uid, int version, const string& app,
                const map<string, CategoryInfo>& categorys, const map<string, TagInfo>& tags);

    int query(const string& uid, int version, string& app,
                bool allCategorys, map<string, CategoryInfo>& categorys,
                bool allTags, map<string, TagInfo>& tags);

public:
    string GetUrl() { return m_url; }
    string GetDBName() { return m_dbName; }
    string GetCollectionName() { return m_collectionName; }

protected:
    int ParseODoc(const string& uid, const bson_t& b, int& version, string& app, 
                        map<string, CategoryInfo>& categorys, map<string, TagInfo>& tags);

    int ParseCategory(const string& uid, bson_t& b, map<string, CategoryInfo>& categorys);
    int ParseOCategory(const string& uid, const string& v, bson_t& b, CategoryInfo& category);
    
    int ParseTag(const string& uid, bson_t& b, map<string, TagInfo>& tags);
    int ParseOTag(const string& uid, const string& v, bson_t& b, TagInfo& tag);

protected:
    string                  m_url;
    string                  m_dbName;
    string                  m_collectionName;
    
    mongoc_client_t*        m_client;
    mongoc_database_t*      m_db;
    mongoc_collection_t*    m_collection; 

protected:
    static bool     m_inited;
    static TLock    m_lockInited;
};

#endif

