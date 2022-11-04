#include <assert.h>

#include "lock.h"

TLock::TLock()
{
    int ret = pthread_mutex_init(&m_mutex, NULL);
    assert(ret == 0);
    m_locked = false;
}

TLock::~TLock()
{
    pthread_mutex_destroy(&m_mutex);
}

bool TLock::TryLock()
{
    if (m_locked)
        return false;

    int ret = pthread_mutex_trylock(&m_mutex);
    if (ret != 0)
    {
        return false;
    }

    m_locked = true;
    return true;
}

bool TLock::Lock()
{
    int ret = pthread_mutex_lock(&m_mutex);
    if (ret != 0)
    {
        return false;
    }

    m_locked = true;
    return true;
}

bool TLock::UnLock()
{
    int ret = pthread_mutex_unlock(&m_mutex);
    if (ret != 0)
    {
        return false;
    }

    m_locked = false;
    return true;
}

