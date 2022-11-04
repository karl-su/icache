#include <stdio.h>

#include "log.h"

CICacheLog __icacheLogObj__;

void ICacheLog(const char* logname, int level, const char* fmt, ...)
{
    va_list ap;
    char msg[ICACHE_LOG_MAX_LEN];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    printf("[%s][%d] %s\n", logname, level, msg);
}
