all: init

CXX := g++
LD := ${CXX}

# Flags to enable link-time optimization and GDB
LTO := -flto
ENABLE_DGB :=

HOTS_HOME := .
MICA_HOME := ../../mica2/src

INC	:= -I ${HOTS_HOME} -I ${MICA_HOME}

#DEBUG := -DNDEBUG
CPPFLAGS := ${ENABLE_DGB} ${LTO} -O2 ${DEBUG} -std=c++14 ${INC} -Wall -Werror -g \
	-Wno-unused-result \
	-Wno-unused-value \
	-Wno-unused-function \
	-Wno-sign-compare \
        -D BOOST_COROUTINES_NO_DEPRECATION_WARNING
	#-Winline

LDFLAGS := ${ENABLE_DGB} ${LTO} -O2 -libverbs -lrt -pthread -lmemcached -lnuma -g \
	-lpapi -lboost_system -lboost_coroutine -lboost_thread

src := ibsetup.o memcached.o \
	mitsume_macro.o mitsume_util.o\
	mitsume_struct.o \
	init.o \
	memory.o client.o server.o \
	mitsume_con_alloc.o mitsume_con_thread.o \
	mitsume_clt_tool.o mitsume_clt_thread.o mitsume_clt_test.o \
	mitsume_tool.o mitsume_tool_cache.o mitsume_tool_gc.o \
	mitsume_stat.o\
	mitsume_benchmark.o

# Handle MICA differently. MICA relies on DNDEBUG to disable datapath asserts.
#MICA_DEBUG := -DNDEBUG
#mica_src := ${MICA_HOME}/mica/util/config.o \
	${MICA_HOME}/mica/util/cityhash/city_mod.o
#${mica_src} : CPPFLAGS += ${MICA_DEBUG}

init: ${src}
	${LD} -o $@ $^ ${LDFLAGS} ${CPPFLAGS}

PHONY: clean
clean:
	rm -f *.o init ${src}

