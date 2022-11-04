#ifndef __LOG_H__
#define __LOG_H__

#include <stdio.h>
#include <stdarg.h>
#include <cstdarg>
#include <iostream>
#include <fstream>
#include <sstream>

using namespace std;

#define ICACHE_LOG_MAX_LEN 4096

#define ICACHE_LOG_DEBUG    1
#define ICACHE_LOG_INFO     2
#define ICACHE_LOG_WARNING  3
#define ICACHE_LOG_ERROR    4

#ifdef FDLOG
#undef FDLOG
#endif
#define FDLOG(x) (__icacheLogObj__ << "[" << x << "][0] [" << __FILE__ << ":" << __LINE__ << "] ")

#ifdef DLOG
#undef DLOG
#endif
#ifdef ILOG
#undef ILOG
#endif
#ifdef ELOG
#undef ELOG
#endif

#define DLOG(fmt, ...) ICacheLog("cache", ICACHE_LOG_DEBUG, "[%s:%d] "fmt, __FILE__, __LINE__, ## __VA_ARGS__) 
#define ILOG(fmt, ...) ICacheLog("cache", ICACHE_LOG_INFO, "[%s:%d] "fmt, __FILE__, __LINE__, ## __VA_ARGS__)
#define ELOG(fmt, ...) ICacheLog("cache", ICACHE_LOG_ERROR, "[%s:%d] "fmt, __FILE__, __LINE__, ## __VA_ARGS__)

void ICacheLog(const char* logname, int level, const char* fmt, ...);

class CICacheLog {
public:    
    template <typename T>
    CICacheLog& operator << (const T& t) { cout << t; return *this; }

    typedef ostream& (*F) (ostream& os);
    CICacheLog& operator << (F f) { (f)(cout); return *this; }

    typedef ios_base& (*I)(ios_base& os);
    CICacheLog& operator << (I f) { (f)(cout); return *this; }
};

extern CICacheLog __icacheLogObj__;

#endif

