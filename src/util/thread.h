#ifndef __THREAD_H__
#define __THREAD_H__

#include <pthread.h>

class ThreadBase {
public:
    pthread_t getid() { return m_id; }
    
    virtual void run() = 0;

    virtual void stop() = 0;

    virtual int start();

protected:
    pthread_t m_id;   
};

#endif

