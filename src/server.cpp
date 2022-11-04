#include "listener.h"
#include "worker.h"
#include "rehasher.h"
#include "asynctask.h"

#include "common/log.h"

#include "tiny-redis/server.h"


int main(int argc, char* argv[])
{
    UNUSED(argc);
    UNUSED(argv);

    FDLOG("icache") << "start" << endl;

    g_redisDB = CreateTinyRedisDB();

    Listener l;
    l.init((char*)"0.0.0.0", 10000);

    Worker w[4];
    for (int i = 0; i < 4; i++)
    {
        w[i].init(g_redisDB);
        l.AddWorker(&w[i]);
        w[i].start();   
    }

    l.start();

    ReHasher h;
    h.init(g_redisDB);
    h.start();

    ASyncTask::Start(2, 1024, 
            "mongodb://192.168.1.235:10000/?connectTimeoutMS=100&socketTimeoutMS=5000", "ufs", "user",
            "192.168.1.17", 8888, 200);

    pthread_join(l.getid(), NULL);
    for (int i = 0; i < 4; i++)
        pthread_join(w[i].getid(), NULL);
    pthread_join(h.getid(), NULL);
    ASyncTask::Stop();
    
    return 0;
}
