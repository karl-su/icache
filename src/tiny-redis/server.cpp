/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"

#include <string.h>
#include <strings.h>
#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>
#include <sys/socket.h>

/*================================= Globals ================================= */

/* Global vars */
TinyRedisDB* g_redisDB;
/* Our command table.
 *
 * Every entry is composed of the following fields:
 *
 * name: a string representing the command name.
 * function: pointer to the C function implementing the command.
 * arity: number of arguments, it is possible to use -N to say >= N
 * sflags: command flags as string. See below for a table of flags.
 * flags: flags as bitmask. Computed by Redis using the 'sflags' field.
 * get_keys_proc: an optional function to get key arguments from a command.
 *                This is only used when the following three fields are not
 *                enough to specify what arguments are keys.
 * first_key_index: first argument that is a key
 * last_key_index: last argument that is a key
 * key_step: step to get all the keys from first to last argument. For instance
 *           in MSET the step is two since arguments are key,val,key,val,...
 * microseconds: microseconds of total execution time for this command.
 * calls: total number of calls of this command.
 *
 * The flags, microseconds and calls fields are computed by Redis and should
 * always be set to zero.
 *
 * Command flags are expressed using strings where every character represents
 * a flag. Later the populateCommandTable() function will take care of
 * populating the real 'flags' field using this characters.
 *
 * This is the meaning of the flags:
 *
 * w: write command (may modify the key space).
 * r: read command  (will never modify the key space).
 * m: may increase memory usage once called. Don't allow if out of memory.
 * a: admin command, like SAVE or SHUTDOWN.
 * p: Pub/Sub related command.
 * f: force replication of this command, regardless of server.dirty.
 * s: command not allowed in scripts.
 * R: random command. Command is not deterministic, that is, the same command
 *    with the same arguments, with the same key space, may have different
 *    results. For instance SPOP and RANDOMKEY are two random commands.
 * S: Sort command output array if called from script, so that the output
 *    is deterministic.
 * l: Allow command while loading the database.
 * t: Allow command while a slave has stale data but is not allowed to
 *    server this data. Normally no command is accepted in this condition
 *    but just a few.
 * M: Do not automatically propagate the command on MONITOR.
 * k: Perform an implicit ASKING for this command, so the command will be
 *    accepted in cluster mode if the slot is marked as 'importing'.
 * F: Fast command: O(1) or O(log(N)) command that should never delay
 *    its execution as long as the kernel scheduler is giving us time.
 *    Note that commands that may trigger a DEL as a side effect (like SET)
 *    are not fast commands.
 */
struct redisCommand redisCommandTable[] = {
    {"get",getCommand,2,"rF",0,1,1,1,0,0},
    {"set",setCommand,-3,"wm",0,1,1,1,0,0},
    {"setnx",setnxCommand,3,"wmF",0,1,1,1,0,0},
    {"setex",setexCommand,4,"wm",0,1,1,1,0,0},
    {"append",appendCommand,3,"wm",0,1,1,1,0,0},
    {"strlen",strlenCommand,2,"rF",0,1,1,1,0,0},
    {"del",delCommand,-2,"w",0,1,-1,1,0,0},
    {"exists",existsCommand,-2,"rF",0,1,-1,1,0,0},
    
	{"hset",hsetCommand,4,"wmF",0,1,1,1,0,0},
    {"hsetnx",hsetnxCommand,4,"wmF",0,1,1,1,0,0},
    {"hget",hgetCommand,3,"rF",0,1,1,1,0,0},
    {"hmset",hmsetCommand,-4,"wm",0,1,1,1,0,0},
    {"hmget",hmgetCommand,-3,"r",0,1,1,1,0,0},
    {"hdel",hdelCommand,-3,"wF",0,1,1,1,0,0},
    {"hlen",hlenCommand,2,"rF",0,1,1,1,0,0},
    {"hstrlen",hstrlenCommand,3,"rF",0,1,1,1,0,0},
    {"hkeys",hkeysCommand,2,"rS",0,1,1,1,0,0},
    {"hvals",hvalsCommand,2,"rS",0,1,1,1,0,0},
    {"hgetall",hgetallCommand,2,"r",0,1,1,1,0,0},
    {"hexists",hexistsCommand,3,"rF",0,1,1,1,0,0},

    {"ttl",ttlCommand,2,"rF",0,1,1,1,0,0},
    {"expire",expireCommand,3,"wF",0,1,1,1,0,0},

    {"cluster",clusterCommand,-2,"a",0,0,0,0,0,0}
};

/*============================ Utility functions ============================ */

/* Low level logging. To use only for very big messages, otherwise
 * serverLog() is to prefer. */
void serverLogRaw(int level, const char *msg) {
    const char *c = ".-*#";
    FILE *fp;
    char buf[64];
    int rawmode = (level & LL_RAW);
    int log_to_stdout = (g_redisDB == NULL || g_redisDB->logfile == NULL || g_redisDB->logfile[0] == '\0');

    level &= 0xff; /* clear flags */
    if (!g_redisDB || level < g_redisDB->verbosity) return;

    fp = log_to_stdout ? stdout : fopen(g_redisDB->logfile,"a");
    if (!fp) return;

    if (rawmode) {
        fprintf(fp,"%s",msg);
    } else {
        int off;
        struct timeval tv;
        int role_char = 'M';

        gettimeofday(&tv,NULL);
        off = strftime(buf,sizeof(buf),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
        fprintf(fp,"%d:%c %s %c %s\n",
            (int)getpid(),role_char, buf,c[level],msg);
    }
    fflush(fp);

    if (!log_to_stdout) fclose(fp);
}

/* Like serverLogRaw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
void serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];

    if (!g_redisDB || (level&0xff) < g_redisDB->verbosity) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level,msg);
}

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void) {
    return ustime()/1000;
}

/*====================== Hash table type implementation  ==================== */

/* This is a hash table type that uses the SDS dynamic strings library as
 * keys and redis objects as values (objects can hold SDS strings,
 * lists, sets). */

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp((const char*)key1, (const char*)key2) == 0;
}

void dictObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */
    decrRefCount((robj*)val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree((sds)val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = (const robj*)key1, *o2 = (const robj*)key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

unsigned int dictObjHash(const void *key) {
    const robj *o = (const robj*)key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

unsigned int dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

unsigned int dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == OBJ_ENCODING_INT &&
        o2->encoding == OBJ_ENCODING_INT)
            return o1->ptr == o2->ptr;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

unsigned int dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == OBJ_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictObjectDestructor   /* val destructor */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
    dictEncObjHash,             /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictEncObjKeyCompare,       /* key compare */
    dictObjectDestructor,  /* key destructor */
    dictObjectDestructor   /* val destructor */
};

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < HASHTABLE_MIN_FILL));
}

unsigned int getLRUClock(void) {
    return (mstime()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* The client query buffer is an sds.c string that can end with a lot of
 * free space not used, this function reclaims space if needed.
 *
 * The function always returns 0 as it never terminates the client. */
/*
 * 调整client的querybuf大小, 为了节约内存
 */
int clientsCronResizeQueryBuffer(client *c) {
    size_t querybuf_size = sdsAllocSize(c->querybuf);
    time_t idletime = c->proc->unixtime - c->lastinteraction;

    /* There are two conditions to resize the query buffer:
     * 1) Query buffer is > BIG_ARG and too big for latest peak.
     * 2) Client is inactive and the buffer is bigger than 1k. */
    if (((querybuf_size > PROTO_MBULK_BIG_ARG) &&
         (querybuf_size/(c->querybuf_peak+1)) > 2) ||
         (querybuf_size > 1024 && idletime > 2))
    {
        /* Only resize the query buffer if it is actually wasting space. */
        if (sdsavail(c->querybuf) > 1024) {
            c->querybuf = sdsRemoveFreeSpace(c->querybuf);
        }
    }
    /* Reset the peak again to capture the peak memory usage in the next
     * cycle. */
    c->querybuf_peak = 0;
    return 0;
}

/* We take a cached value of the unix time in the global state because with
 * virtual memory and aging there is to store the current time in objects at
 * every object access, and accuracy is not needed. To access a global var is
 * a lot faster than calling time(NULL) */
void updateCachedTime(TinyRedisProc* proc) {
    proc->unixtime = time(NULL);
    proc->mstime = mstime();
}

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeSleep(struct aeEventLoop *eventLoop) {
    UNUSED(eventLoop);

    /* Handle writes with pending output buffers. */
    handleClientsWithPendingWrites((TinyRedisProc*)eventLoop->clientData);
}

/* =========================== Server initialization ======================== */

void InitSharedObjects(TinyRedisDB* db) {
    int j;

    db->shared.crlf = createObject(OBJ_STRING,sdsnew("\r\n"));
    db->shared.ok = createObject(OBJ_STRING,sdsnew("+OK\r\n"));
    db->shared.err = createObject(OBJ_STRING,sdsnew("-ERR\r\n"));
    db->shared.emptybulk = createObject(OBJ_STRING,sdsnew("$0\r\n\r\n"));
    db->shared.czero = createObject(OBJ_STRING,sdsnew(":0\r\n"));
    db->shared.cone = createObject(OBJ_STRING,sdsnew(":1\r\n"));
    db->shared.cnegone = createObject(OBJ_STRING,sdsnew(":-1\r\n"));
    db->shared.nullbulk = createObject(OBJ_STRING,sdsnew("$-1\r\n"));
    db->shared.nullmultibulk = createObject(OBJ_STRING,sdsnew("*-1\r\n"));
    db->shared.emptymultibulk = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    db->shared.pong = createObject(OBJ_STRING,sdsnew("+PONG\r\n"));
    db->shared.queued = createObject(OBJ_STRING,sdsnew("+QUEUED\r\n"));
    db->shared.emptyscan = createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
    db->shared.wrongtypeerr = createObject(OBJ_STRING,sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
    db->shared.nokeyerr = createObject(OBJ_STRING,sdsnew(
        "-ERR no such key\r\n"));
    db->shared.syntaxerr = createObject(OBJ_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    db->shared.sameobjecterr = createObject(OBJ_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    db->shared.outofrangeerr = createObject(OBJ_STRING,sdsnew(
        "-ERR index out of range\r\n"));
    db->shared.noscripterr = createObject(OBJ_STRING,sdsnew(
        "-NOSCRIPT No matching script. Please use EVAL.\r\n"));
    db->shared.loadingerr = createObject(OBJ_STRING,sdsnew(
        "-LOADING Redis is loading the dataset in memory\r\n"));
    db->shared.slowscripterr = createObject(OBJ_STRING,sdsnew(
        "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
    db->shared.masterdownerr = createObject(OBJ_STRING,sdsnew(
        "-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n"));
    db->shared.bgsaveerr = createObject(OBJ_STRING,sdsnew(
        "-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk. Commands that may modify the data set are disabled. Please check Redis logs for details about the error.\r\n"));
    db->shared.roslaveerr = createObject(OBJ_STRING,sdsnew(
        "-READONLY You can't write against a read only slave.\r\n"));
    db->shared.noautherr = createObject(OBJ_STRING,sdsnew(
        "-NOAUTH Authentication required.\r\n"));
    db->shared.oomerr = createObject(OBJ_STRING,sdsnew(
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
    db->shared.execaborterr = createObject(OBJ_STRING,sdsnew(
        "-EXECABORT Transaction discarded because of previous errors.\r\n"));
    db->shared.noreplicaserr = createObject(OBJ_STRING,sdsnew(
        "-NOREPLICAS Not enough good slaves to write.\r\n"));
    db->shared.busykeyerr = createObject(OBJ_STRING,sdsnew(
        "-BUSYKEY Target key name already exists.\r\n"));
    db->shared.space = createObject(OBJ_STRING,sdsnew(" "));
    db->shared.colon = createObject(OBJ_STRING,sdsnew(":"));
    db->shared.plus = createObject(OBJ_STRING,sdsnew("+"));

    for (j = 0; j < PROTO_SHARED_SELECT_CMDS; j++) {
        char dictid_str[64];
        int dictid_len;

        dictid_len = ll2string(dictid_str,sizeof(dictid_str),j);
        db->shared.select[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, dictid_str));
    }
    db->shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
    db->shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
    db->shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
    db->shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
    db->shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
    db->shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);
    db->shared.del = createStringObject("DEL",3);
    db->shared.rpop = createStringObject("RPOP",4);
    db->shared.lpop = createStringObject("LPOP",4);
    db->shared.lpush = createStringObject("LPUSH",5);
    for (j = 0; j < OBJ_SHARED_INTEGERS; j++) {
        db->shared.integers[j] = createObject(OBJ_STRING,(void*)(long)j);
        db->shared.integers[j]->encoding = OBJ_ENCODING_INT;
    }
    for (j = 0; j < OBJ_SHARED_BULKHDR_LEN; j++) {
        db->shared.mbulkhdr[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"*%d\r\n",j));
        db->shared.bulkhdr[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"$%d\r\n",j));
    }
    /* The following two shared objects, minstring and maxstrings, are not
     * actually used for their value but as a special object meaning
     * respectively the minimum possible string and the maximum possible
     * string in string comparisons for the ZRANGEBYLEX command. */
    db->shared.minstring = createStringObject("minstring",9);
    db->shared.maxstring = createStringObject("maxstring",9);
}

TinyRedisDB* CreateTinyRedisDB()
{
    TinyRedisDB* db = (TinyRedisDB*)zmalloc(sizeof(TinyRedisDB));
    db->dbnum = CONFIG_DEFAULT_DBNUM;
    db->db  = (redisDb*)zmalloc(sizeof(redisDb) * db->dbnum);
    for (int i = 0; i < db->dbnum; i++)
    {
        db->db[i].d = dictCreate(&dbDictType, NULL);
        db->db[i].id = i;
        db->db[i].avg_ttl = 0;

        db->db[i].rwlock = PTHREAD_RWLOCK_INITIALIZER;
    }
    
    InitSharedObjects(db);

    db->system_memory_size = zmalloc_get_memory_size();
    db->client_max_querybuf_len = PROTO_MAX_QUERYBUF_LEN;
    db->verbosity = LL_DEBUG;
    db->logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
    db->maxclients = CONFIG_DEFAULT_MAX_CLIENTS;
    db->maxmemory = CONFIG_DEFAULT_MAXMEMORY;
    db->hash_max_ziplist_entries = OBJ_HASH_MAX_ZIPLIST_ENTRIES;
    db->hash_max_ziplist_value = OBJ_HASH_MAX_ZIPLIST_VALUE;

    db->lruclock = getLRUClock();
    
    /* Client output buffer limits */
    for (int j = 0; j < CLIENT_TYPE_OBUF_COUNT; j++)
        db->client_obuf_limits[j] = clientBufferLimitsDefaults[j];

    /* Command table -- we initiialize it here as it is part of the
     * initial configuration, since command names may be changed via
     * redis.conf using the rename-command directive. */
    db->commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable(db);
    
    /* Debugging */
    db->assert_failed = "<no assertion failed>";
    db->assert_file = "<no file>";
    db->assert_line = 0;

    return db;
}

TinyRedisProc* CreateTinyRedisProc(TinyRedisDB* db, int read_fd, int write_fd)
{
    TinyRedisProc* proc = (TinyRedisProc*)zmalloc(sizeof(TinyRedisProc));
    proc->notify_fd_read = read_fd;
    proc->notify_fd_write = write_fd;
    proc->el = aeCreateEventLoop(db->maxclients + CONFIG_FDSET_INCR, proc);
    if (aeCreateFileEvent(proc->el, read_fd, AE_READABLE, NotifyHandle, proc) == AE_ERR)
    {
        zfree(proc->el);
        zfree(proc);
        return NULL;
    }

    aeSetBeforeSleepProc(proc->el, beforeSleep);

    proc->current_client = NULL;
    proc->clients = listCreate();
    proc->clients_to_close = listCreate();
    proc->clients_pending_write = listCreate();
    proc->next_client_id = 1; /* Client IDs, start from 1 .*/
    
    updateCachedTime(proc);

    proc->db = db;

    return proc;
}

/* Populates the Redis Command Table starting from the hard coded list
 * we have on top of redis.c file. */
void populateCommandTable(TinyRedisDB* db) {
    int j;
    int numcommands = sizeof(redisCommandTable)/sizeof(struct redisCommand);

    for (j = 0; j < numcommands; j++) {
        struct redisCommand *c = redisCommandTable+j;
        const char *f = c->sflags;
        int retval;

        while(*f != '\0') {
            switch(*f) {
            case 'w': c->flags |= CMD_WRITE; break;
            case 'r': c->flags |= CMD_READONLY; break;
            case 'm': c->flags |= CMD_DENYOOM; break;
            case 'a': c->flags |= CMD_ADMIN; break;
            case 'p': c->flags |= CMD_PUBSUB; break;
            case 's': c->flags |= CMD_NOSCRIPT; break;
            case 'R': c->flags |= CMD_RANDOM; break;
            case 'S': c->flags |= CMD_SORT_FOR_SCRIPT; break;
            case 'l': c->flags |= CMD_LOADING; break;
            case 't': c->flags |= CMD_STALE; break;
            case 'M': c->flags |= CMD_SKIP_MONITOR; break;
            case 'k': c->flags |= CMD_ASKING; break;
            case 'F': c->flags |= CMD_FAST; break;
            default: serverPanic("Unsupported command flag"); break;
            }
            f++;
        }

        retval = dictAdd(db->commands, sdsnew(c->name), c);
        serverAssert(retval == DICT_OK);
    }
}

void resetCommandTableStats(void) {
    int numcommands = sizeof(redisCommandTable)/sizeof(struct redisCommand);
    int j;

    for (j = 0; j < numcommands; j++) {
        struct redisCommand *c = redisCommandTable+j;

        c->microseconds = 0;
        c->calls = 0;
    }
}

/* ====================== Commands lookup and execution ===================== */

struct redisCommand *lookupCommand(TinyRedisProc* proc, sds name) {
    return (redisCommand*)dictFetchValue(proc->db->commands, name);
}

/* Call() is the core of Redis execution of a command.
 *
 * The following flags can be passed:
 * CMD_CALL_NONE        No flags.
 * CMD_CALL_STATS       Populate command stats.
 * CMD_CALL_PROPAGATE_AOF   Append command to AOF if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE_REPL  Send command to salves if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE   Alias for PROPAGATE_AOF|PROPAGATE_REPL.
 * CMD_CALL_FULL        Alias for SLOWLOG|STATS|PROPAGATE.
 *
 * The exact propagation behavior depends on the client flags.
 * Specifically:
 *
 * 1. If the client flags CLIENT_FORCE_AOF or CLIENT_FORCE_REPL are set
 *    and assuming the corresponding CMD_CALL_PROPAGATE_AOF/REPL is set
 *    in the call flags, then the command is propagated even if the
 *    dataset was not affected by the command.
 * 2. If the client flags CLIENT_PREVENT_REPL_PROP or CLIENT_PREVENT_AOF_PROP
 *    are set, the propagation into AOF or to slaves is not performed even
 *    if the command modified the dataset.
 *
 * Note that regardless of the client flags, if CMD_CALL_PROPAGATE_AOF
 * or CMD_CALL_PROPAGATE_REPL are not set, then respectively AOF or
 * slaves propagation will never occur.
 *
 * Client flags are modified by the implementation of a given command
 * using the following API:
 *
 * forceCommandPropagation(client *c, int flags);
 * preventCommandPropagation(client *c);
 * preventCommandAOF(client *c);
 * preventCommandReplication(client *c);
 *
 */
void call(client *c) {
    long long dirty, start, duration;
	
    /* Initialization: clear the flags that must be set by the command on
     * demand, and initialize the array for additional commands propagation. */
    c->flags &= ~(CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);

    /* Call the command. */
    dirty = c->db->dirty;
    start = ustime();
    c->cmd->proc(c);
    duration = ustime()-start;
    dirty = c->db->dirty-dirty;
    if (dirty < 0) dirty = 0;

    c->lastcmd->microseconds += duration;
    c->lastcmd->calls++;
}

unsigned int keyHashSlot(const char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing betweeen {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return crc16(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     *      * what is in the middle between { and }. */
    return crc16(key+s+1,e-s-1) & 0x3FFF;
}

int* getKeysFromCommand(struct redisCommand *cmd,robj **argv, int argc, int *numkeys) {
    int j, i = 0, last, *keys;
    UNUSED(argv);

    if (cmd->firstkey == 0) {
        *numkeys = 0;
        return NULL;
    }

    last = cmd->lastkey;
    if (last < 0) last = argc+last;
    keys = (int*)zmalloc(sizeof(int)*((last - cmd->firstkey)+1));
    for (j = cmd->firstkey; j <= last; j += cmd->keystep) {
        if (j >= argc) {
            serverPanic("Redis built-in command declared keys positions not matching the arity requirements.");
        }
        keys[i++] = j;
    }
    *numkeys = i;
    return keys;
}

int getDBIndex(client *c, struct redisCommand *cmd, robj **argv, int argc) 
{
    UNUSED(c);

    int i, slot = -1;
    int *keyindex, numkeys;

    keyindex = getKeysFromCommand(cmd, argv, argc, &numkeys);
    for (i = 0; i < numkeys; i++) {
        robj *thiskey = argv[keyindex[i]];
        int thisslot = keyHashSlot((const char*)thiskey->ptr,
                sdslen((sds)thiskey->ptr));
        if (slot >= 0 && thisslot != slot)
        {
            slot = -2;
            break;
        }
        slot = thisslot;
    }

    /* 0x4000定义为配置数据库 */
    if (numkeys == 0)
        slot = 0x4000;

    if (keyindex)
        zfree(keyindex);

    return slot;
}

/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If C_OK is returned the client is still alive and valid and
 * other operations can be performed by the caller. Otherwise
 * if C_ERR is returned the client was destroyed (i.e. after QUIT). */
int processCommand(client *c) {
    /* The QUIT command is handled separately. Normal command procs will
     * go through checking for replication and QUIT will cause trouble
     * when FORCE_REPLICATION is enabled and would be implemented in
     * a regular command proc. */
    if (!strcasecmp((const char*)c->argv[0]->ptr,"quit")) {
        addReply(c,c->proc->db->shared.ok);
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        return C_ERR;
    }
    
    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    c->cmd = c->lastcmd = lookupCommand(c->proc, (sds)c->argv[0]->ptr);
    if (!c->cmd) {
        serverLog(LL_DEBUG, "return unknow command '%s'", (char*)c->argv[0]->ptr);
        addReplyErrorFormat(c,"unknown command '%s'",
            (char*)c->argv[0]->ptr);
        return C_OK;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return C_OK;
    }


    int dbIndex = getDBIndex(c, c->cmd, c->argv, c->argc);
    if (dbIndex < 0 || dbIndex >= c->proc->db->dbnum)
    {
        addReplyErrorFormat(c,"unknown operate db '%d'", dbIndex);
        return C_OK;
    }
    c->db = &c->proc->db->db[dbIndex];

    if (c->cmd->flags & CMD_WRITE)
        pthread_rwlock_wrlock(&c->db->rwlock);
    else
        pthread_rwlock_rdlock(&c->db->rwlock);
    call(c);
    pthread_rwlock_unlock(&c->db->rwlock);
    
    return C_OK;
}

void usage(void) {
    fprintf(stderr,"Usage: ./redis-server [/path/to/redis.conf] [options]\n");
    fprintf(stderr,"       ./redis-server - (read config from stdin)\n");
    fprintf(stderr,"       ./redis-server -v or --version\n");
    fprintf(stderr,"       ./redis-server -h or --help\n");
    fprintf(stderr,"       ./redis-server --test-memory <megabytes>\n\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./redis-server (run the server with default conf)\n");
    fprintf(stderr,"       ./redis-server /etc/redis/6379.conf\n");
    fprintf(stderr,"       ./redis-server --port 7777\n");
    fprintf(stderr,"       ./redis-server --port 7777 --slaveof 127.0.0.1 8888\n");
    fprintf(stderr,"       ./redis-server /etc/myredis.conf --loglevel verbose\n\n");
    fprintf(stderr,"Sentinel mode:\n");
    fprintf(stderr,"       ./redis-server /etc/sentinel.conf --sentinel\n");
    exit(1);
}

void redisOutOfMemoryHandler(size_t allocation_size) {
    serverLog(LL_WARNING,"Out Of Memory allocating %zu bytes!",
        allocation_size);
    serverPanic("Redis aborting for OUT OF MEMORY");
}

#if 0
void* func(void* arg)
{
    TinyRedisProc* proc = (TinyRedisProc*)arg;

    aeMain(proc->el);
}

int main(int argc, char **argv) {
    struct timeval tv;

    setlocale(LC_COLLATE,"");
    zmalloc_enable_thread_safeness();
    zmalloc_set_oom_handler(redisOutOfMemoryHandler);
    srand(time(NULL)^getpid());
    gettimeofday(&tv,NULL);
    dictSetHashFunctionSeed(tv.tv_sec^tv.tv_usec^getpid());

    g_redisDB = CreateTinyRedisDB();
    /* Warning the user about suspicious maxmemory setting. */
    if (g_redisDB->maxmemory > 0 && g_redisDB->maxmemory < 1024*1024) {
        serverLog(LL_WARNING,"WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?", g_redisDB->maxmemory);
    }


    int p[2] = {-1, -1};
    pipe(p);
    anetNonBlock(NULL,p[0]);
    anetNonBlock(NULL,p[1]);
    TinyRedisProc* proc = CreateTinyRedisProc(g_redisDB, p[0], p[1]);

    int fd = anetTcpServer(proc->neterr, 10000, NULL, 10);
    aeEventLoop* el = aeCreateEventLoop(CONFIG_FDSET_INCR, NULL);
    aeCreateFileEvent(el, fd, AE_READABLE, acceptTcpHandler, proc);

    pthread_t thread_id;
    pthread_create(&thread_id, NULL, func, proc);  

    aeMain(el);
#if 0
    aeSetBeforeSleepProc(server.el,beforeSleep);
    aeMain(server.el);
    aeDeleteEventLoop(server.el);
#endif

    printf("over!\n");
    return 0;
}
#endif

/* The End */
