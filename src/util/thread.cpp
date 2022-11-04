#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <assert.h>

#include "thread.h"

static void* thread_func(void* arg)
{
    assert(arg);

    ThreadBase* pThread = (ThreadBase*)arg;
    pThread->run();

    return NULL;
}

int ThreadBase::start()
{
    int ret = pthread_create(&m_id, NULL, thread_func, this);
    if (ret != 0)
    {
        fprintf(stderr, "pthread_create failed! ret: %d, errno: %d, error: %s\n", ret, errno, strerror(errno));
        return ret;
    }

    return 0;
}
