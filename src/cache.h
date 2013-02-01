
/* tmpcache/cache.h */

#ifndef TMPCACHE_CACHE_H
#define TMPCACHE_CACHE_H 1

#include "../config.h"

#include <stdint.h>
#include <stdlib.h>

#include "utility.h"

#if defined HAVE_LIBCDB
#include <cdb.h>
#include <fcntl.h>

typedef struct cdb cdb_t;
typedef struct cdb_make cdbm_t;
#endif

/* memory.c : */
void *c_malloc (size_t size,void *hint);
void  c_free   (void *p,void *hint);


typedef uint64_t (*c_readf) (void *,bstring,char *,uint64_t);
typedef uint64_t (*c_writef) (bstring,bstring,char *,uint64_t, uint64_t);
typedef uint32_t (*c_signalf) (void);

#if defined HAVE_LIBCDB
uint32_t c_iscdbfile (bstring cachepath);
#endif

uint32_t c_filterkey (bstring key);



typedef struct {

  bstring address;
  bstring cachepath;
  uint64_t size;

  c_signalf signalf;
} tc_readconfig_t;

typedef struct {

  bstring address;
  bstring cachepath;
  uint64_t size;
  uint64_t maxsize;

  c_signalf signalf;
} tc_writeconfig_t;

typedef struct {

  bstring address;
  bstring cachepath;

} tc_snapshotconfig_t;

void tc_readfromcache (tc_readconfig_t *);
void tc_writefromcache (tc_writeconfig_t *);
void tc_snapshotcache (tc_snapshotconfig_t *);

#endif
