
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
typedef void (*c_errorf) (const char *);

#if defined HAVE_LIBCDB
uint32_t c_iscdbfile (bstring cachepath);
#endif

uint32_t c_filterkey (bstring key);

typedef struct {

  bstring address;
  bstring cachepath;
  uint64_t size;
  uint32_t miss;

  c_signalf signalf;
  c_errorf errorf;

} tc_readconfig_t;

typedef struct {

  uint64_t numofreads;
  uint64_t numofmisses;
  
} tc_readinfo_t;

typedef struct {

  bstring address;
  bstring cachepath;
  uint64_t size;
  uint64_t maxsize;

  c_signalf signalf;
  c_errorf errorf;

} tc_writeconfig_t;

typedef struct {

  uint64_t numofwrites;
  uint64_t largestwrite;
  uint64_t lowestwrite;

} tc_writeinfo_t;

typedef struct {

  bstring address;
  bstring cachepath;

  c_signalf signalf;
  c_errorf errorf;

} tc_snapshotconfig_t;

typedef struct {
  
  uint64_t numof;
  uint64_t largestsize;
  uint64_t lowestsize;

} tc_snapshotinfo_t; /* MAYBE: condense all info structs into a single one*/

tc_readinfo_t *     tc_readfromcache          (tc_readconfig_t *);
tc_writeinfo_t *    tc_writefromcache         (tc_writeconfig_t *);
tc_snapshotinfo_t * tc_snapshotcachetostdout  (tc_snapshotconfig_t *);
tc_snapshotinfo_t * tc_snapshotcachetoaddress (tc_snapshotconfig_t *);

#endif
