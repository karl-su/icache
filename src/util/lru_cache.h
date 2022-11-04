#ifndef __LRU_CACHE_H__
#define __LRU_CACHE_H__

#include <iostream>
#include <map>
#include <list>
#include "util/lock.h"

template<class K, class V>
class LRUCache {

public:
    typedef K KeyType;
    typedef V ValueType;
    typedef std::list<KeyType> Tracker;
    typedef std::map<KeyType, std::pair<ValueType, typename Tracker::iterator> > Storage;

public:
    LRUCache(uint32_t capacity = 128 * 1024);
    virtual ~LRUCache();

    void reset(uint32_t capacity);

    int get(const KeyType& key, ValueType& val);

    int set(const KeyType& key, const ValueType& val);

protected:
    Tracker             tracker_;
    Storage             storage_;
    TLock               lock_;


    uint32_t            capacity_;    
    uint32_t            nstab_;
};

template<class K, class V>
LRUCache<K, V>::LRUCache(uint32_t capacity)
{
    reset(capacity);
}

template<class K, class V>
LRUCache<K, V>::~LRUCache()
{
    tracker_.clear();
    storage_.clear();
}

template<class K, class V>
void LRUCache<K, V>::reset(uint32_t capacity)
{
    assert(capacity);

    capacity_ = capacity;
    
    tracker_.clear();
    storage_.clear();

    nstab_ = (uint32_t)(capacity * 0.8);
    nstab_ = (nstab_ == 0) ? 1 : nstab_;
}

template<class K, class V>
int LRUCache<K, V>::get(const KeyType& key, ValueType& val)
{
    Guard g(lock_);

    typename Storage::iterator it = storage_.find(key);
    if (it == storage_.end())
    {
        std::cout << "No Key : " << key << std::endl;
        return 0;
    }

    std::cout << "Hit Key : " << key << std::endl;
    tracker_.splice(tracker_.end(), tracker_, it->second.second);

    val = it->second.first;

    return 1;
}

template<class K, class V>
int LRUCache<K, V>::set(const KeyType& key, const ValueType& val)
{
    Guard g(lock_);

    typename Storage::iterator it = storage_.find(key);
    if (it != storage_.end())
    {
        std::cout << "Has Key : " << key << std::endl;
        it->second.first = val;
        tracker_.splice(tracker_.end(), tracker_, it->second.second);
        return 0;
    }

    if (storage_.size() >= capacity_)
    {
        while (storage_.size() >= nstab_)
        {
            assert(!tracker_.empty());

            const typename Storage::iterator it = storage_.find(tracker_.front());

            assert(it != storage_.end());

            std::cout << "Erase Key : " << tracker_.front() << std::endl;
            storage_.erase(it);
            tracker_.pop_front();
        }
    }

    std::cout << "Set key : " << key << std::endl;
    typename Tracker::iterator itTracker = tracker_.insert(tracker_.end(), key);
    storage_.insert(std::make_pair(key, std::make_pair(val, itTracker)));
    
    return 1;
}

#endif

