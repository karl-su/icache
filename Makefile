OPTIMIZATION?=-O0

# Default settings
STD= -std=c++0x -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DUSE_JEMALLOC
WARN=-Wall -W
OPT=$(OPTIMIZATION)

CFLAGS= -Wno-deprecated -Wno-unused-parameter -Wno-literal-suffix \
	-Idep/jemalloc/include \
	-Isrc -Isrc/tiny-redis \
	-I/usr/local/inutil \
	-I/usr/local/include \
	-I/usr/local/include/libmongoc-1.0 \
	-I/usr/local/include/libbson-1.0 \
	-I/usr/local/include/hiredis/

DEBUG=-g -ggdb

FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS)  
FINAL_LDFLAGS=$(LDFLAGS) $(DEBUG)
FINAL_LIBS=-lm

FINAL_LIBS+= -L/usr/local/lib/ \
	/usr/local/inutil/libinvutil.a  -levent  \
	-lrt -lhiredis /usr/local/lib/libcityhash.a \
	-lz  -lm -lpthread -lssl -ldb \
	-L/usr/lib/x86_64-linux-gnu/ -ldl \
	/usr/local/lib/libmongoc-1.0.a \
	/usr/local/lib/libbson-1.0.a \
	/usr/local/lib/libsasl2.a \
	/usr/local/lib/librdkafka.a \
	/usr/local/lib/libmonitor.a \
	/usr/local/qconf/lib/libqconf.a \
	dep/jemalloc/lib/libjemalloc.a \
	-lhiredis \
	/usr/local/co/lib/libco.a 

FINAL_LDFLAGS+= -rdynamic
FINAL_LIBS+=-ldl -lpthread

CC=g++
LD=g++

ICACHE_MAIN=icache
ICACHE_OBJ= \
		src/server.o src/listener.o src/worker.o src/rehasher.o src/asynctask.o \
		src/common/log.o \
		src/util/util.o src/util/thread.o src/util/lock.o \
		\
		src/common/mongo_cli.o \
		\
		src/tiny-redis/adlist.o src/tiny-redis/ae.o src/tiny-redis/anet.o \
		src/tiny-redis/dict.o \
		src/tiny-redis/server.o src/tiny-redis/sds.o src/tiny-redis/zmalloc.o \
		src/tiny-redis/networking.o src/tiny-redis/util.o src/tiny-redis/ziplist.o \
		src/tiny-redis/object.o src/tiny-redis/db.o src/tiny-redis/t_string.o\
	   	src/tiny-redis/t_hash.o src/tiny-redis/config.o src/tiny-redis/crc16.o \
		src/tiny-redis/rand.o src/tiny-redis/crc64.o src/tiny-redis/debug.o \
		src/tiny-redis/endianconv.o src/tiny-redis/cluster.o

all: $(ICACHE_MAIN) 

.PHONY: all

# redis-server
$(ICACHE_MAIN): $(ICACHE_OBJ)
	$(LD) $(FINAL_LDFLAGS) -o $@ $^ $(FINAL_LIBS)

$(ICACHE_OBJ) : %.o : %.cpp
	$(CC) $(FINAL_CFLAGS) -c $< -o $@

clean:
	rm -rf $(ICACHE_MAIN) src/*.o src/tiny-redis/*.o \
		src/util/*.o src/common/*.o 

