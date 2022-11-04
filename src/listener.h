#ifndef __LISTENER_H__ 
#define __LISTENER_H__

#include <vector>
#include "worker.h"

#include "tiny-redis/anet.h"
#include "tiny-redis/ae.h"

using namespace std;

class Listener : public ThreadBase {
public:
    int init(char* bindaddr, int port);

    int AddWorker(Worker* w);

    virtual void run();

    virtual void stop();

    static void AcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
protected:  
    int                     m_listenFd;
    aeEventLoop*            m_el;

    vector<Worker*>         m_workers; 
};

#endif

