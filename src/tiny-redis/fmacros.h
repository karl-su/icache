#ifndef _REDIS_FMACRO_H
#define _REDIS_FMACRO_H

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#if defined(__linux__)
    #ifndef _GNU_SOURCE
    #define _GNU_SOURCE
    #endif

    #ifndef _DEFAULT_SOURCE
    #define _DEFAULT_SOURCE
    #endif
#endif

#if defined(_AIX)
#define _ALL_SOURCE
#endif

#if defined(__linux__) || defined(__OpenBSD__)
#define _XOPEN_SOURCE 700

#elif !defined(__NetBSD__)
#define _XOPEN_SOURCE
#endif

#if defined(__sun)
#define _POSIX_C_SOURCE 199506L
#endif

#ifndef _LARGEFILE_SOURCE
#define _LARGEFILE_SOURCE
#endif

#define _FILE_OFFSET_BITS 64

#endif
