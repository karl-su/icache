#include <assert.h>
#include "common/log.h"
#include "listener.h"

int Listener::init(char* bindaddr, int port)
{
    char err[128] = {0};

    int fd = anetTcpServer(err, port, bindaddr, 32);
    if (fd == ANET_ERR)
    {
        ELOG("listen failed! addr: %s, port: %d, error: %s", 
                bindaddr, port, err);
        return -1;
    }
    m_listenFd = fd;

    m_el = aeCreateEventLoop(32, NULL);
    aeCreateFileEvent(m_el, fd, AE_READABLE, Listener::AcceptHandler, this);

    return 0;
}

int Listener::AddWorker(Worker* w)
{
    m_workers.push_back(w);

    return (int)m_workers.size();
}

void Listener::AcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) 
{
    char neterr[128] = {0};
    int cport, cfd, max = 32;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);

    static uint64_t index = 0;

    Listener* l = (Listener*)privdata;

    while(max--) {
        DLOG("To Accepted ...");
        cfd = anetTcpAccept(neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                ELOG("Accepting client connection: %s", neterr);
            return;
        }
        DLOG("Accepted %s:%d", cip, cport);

        NotifyInfo info;
        info.fd = cfd;
        info.ms = 0;
        write(l->m_workers[index++ % l->m_workers.size()]->redis()->notify_fd_write, (void*)&info, sizeof(info));
    }
}

void Listener::run()
{
    assert(m_el);

    aeMain(m_el);
}

void Listener::stop()
{
    if (m_el)
        aeStop(m_el);
}
