
/* tmpcache/cache.h */

#ifndef TMPCACHE_CACHE_H
#define TMPCACHE_CACHE_H 1

#include "../config.h"

#include <stdlib.h>
#include "bstrlib/bstrlib.h"

#if defined HAVE_LIBCDB
#include <cdb.h>
#include <fcntl.h>

typedef struct cdb cdb_t;
typedef struct cdb_make cdbm_t;
#endif

/* memory.c : */
void *c_malloc (unsigned int size,void *hint);
void  c_free   (void *p,void *hint);

typedef unsigned int (*c_readf) (void *,bstring,char *,unsigned int);
typedef unsigned int (*c_writef) (bstring,char *,unsigned int,char *,unsigned int);
typedef unsigned int (*c_signalf) (void);

#if defined HAVE_LIBCDB
int c_iscdbfile (bstring cachepath);
#endif


void c_readfromcache (bstring address,bstring cachepath,int maxsize,c_signalf signal);
void c_writefromcache (bstring address,bstring cachepath,int maxsize,c_signalf signal);

#endif
