#ifndef __LOCK_H__
#define __LOCK_H__

#include <pthread.h>

class LockBase {
public:
    virtual bool TryLock() { return false; }
    virtual bool Lock() { return false; };
    virtual bool UnLock() { return false; };
};

class Guard {
public:
    Guard(LockBase& l) : m_lock(l) { m_lock.Lock(); }
    ~Guard() { m_lock.UnLock(); };

protected:
    LockBase&  m_lock;
};

class TLock : public LockBase {
public:
    TLock();
    ~TLock();

    bool TryLock();
    bool Lock();
    bool UnLock();

protected:
    pthread_mutex_t m_mutex;
    bool            m_locked;
};

#endif
