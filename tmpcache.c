
#include "tmpcache.h"
#include "PMurHash.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h> /* FIXME, just for testing */

#include <xs/xs.h>

#define DEFAULT_CACHES 5

#define HASH_SEED 101
#define tc_hash(s,l) PMurHash32(HASH_SEED,(s),(l))

void * tc_malloc (int size, void *hint) {

  return malloc (size);
}

void tc_free (void * ptr, void *hint) {

  free (ptr);
}

int tc_choosecache (const char *key,const int klen,int *cache_hashes,int numof) {
  
  int hash = tc_hash(key,klen); 
  return cache_hashes[(hash * hash) % numof];
}

typedef struct {

  int rhash;
  char *raddress;
  int rlen;

  void * read;
  int rinterface;

  int whash;
  char *waddress;
  int wlen;

  void * write;
  int winterface;

} address_t;

typedef struct {

  void *xsctx;

  address_t *caches;
  int numof;

  int *readhashes;
  int numof_reads;
  
  int *writehashes;
  int numof_writes;

  tmpcache_mallocf mallocf;
  tmpcache_freef   freef;
  void * hint;

  tmpcache_choosef readf;
  tmpcache_choosef writef;
  tmpcache_choosef deletef;

} ctx_t;

address_t * allocaddresses(address_t *src,int onumof,int numof,
			   tmpcache_mallocf mallocf, 
			   tmpcache_freef freef,void *hint) 
{
  address_t * a = (address_t *)(*mallocf)(sizeof(address_t) * numof,hint); /* FIXME */
  assert(a); /* FIXME */
  int i;
  for (i=0;i < numof;i++) {
    
    if (!src || onumof <= i) {
      
      a[i].rhash = 0;
      a[i].whash = 0;
      a[i].raddress = NULL;
      a[i].waddress = NULL;
      a[i].wlen = 0;
      a[i].rlen = 0;
      a[i].read = NULL;
      a[i].write = NULL;
      a[i].rinterface = -1;
      a[i].winterface = -1;

    } else if (onumof > i) {
      
      a[i].rhash = src[i].rhash;
      a[i].whash = src[i].whash;
      a[i].raddress = src[i].raddress;
      a[i].waddress = src[i].waddress;
      a[i].rlen = src[i].rlen;
      a[i].wlen = src[i].wlen;
      a[i].read = src[i].read;
      a[i].write = src[i].write;
      a[i].rinterface = src[i].rinterface;
      a[i].winterface = src[i].winterface;
    }
  }

  if (src)
    (*freef) (src,hint); /* FIXME */

  return a;
}

int tmpcache_hash (const char *key,const int klen) 
{ 
  return tc_hash(key,klen);
}

void * tmpcache_init (void) 
{
  return tmpcache_custom (tc_malloc,tc_free,tc_choosecache,tc_choosecache,tc_choosecache,NULL);
}

void * tmpcache_custom (tmpcache_mallocf mallocf,tmpcache_freef freef, void *hint,
			tmpcache_choosef readf,tmpcache_choosef writef,tmpcache_choosef deletef) 
{
  
  ctx_t *ctx = (ctx_t *)(*mallocf) (sizeof(ctx_t),hint); /* FIXME, check mallocf first! */
  assert (ctx); /* FIXME */
 
  ctx->mallocf = (mallocf) ? mallocf : tc_malloc;
  ctx->freef = (freef) ? freef : tc_free;
  ctx->hint = hint;
  ctx->readf = (readf) ? readf : tc_choosecache;
  ctx->writef = (writef) ? writef : tc_choosecache;
  ctx->deletef = (deletef) ? deletef : tc_choosecache;

  ctx->xsctx = NULL;

  ctx->caches = allocaddresses(NULL,0,DEFAULT_CACHES,ctx->mallocf,ctx->freef,ctx->hint);
  ctx->numof = DEFAULT_CACHES;

  ctx->readhashes = NULL;
  ctx->numof_reads = 0;

  ctx->writehashes = NULL;
  ctx->numof_writes = 0;

  return (void *)ctx;
}

int tmpcache_open (void *_ctx)
{
  ctx_t * ctx = (ctx_t *)_ctx;
  ctx->xsctx = xs_init();
  assert (ctx->xsctx); /* FIXME */
 
  return 0;
}

int tmpcache_close (void *_ctx)
{
  ctx_t *ctx = (ctx_t *)_ctx;
  int r = xs_term (ctx->xsctx);
  assert (r == 0); /* FIXME */
  return 0;
}


int tmpcache_term (void *_ctx)
{
  ctx_t *ctx = (ctx_t *)_ctx;

  int i;
  for (i=0;i < ctx->numof;i++) {
    if (ctx->caches[i].wlen) 
      (*ctx->freef)(ctx->caches[i].waddress,ctx->hint);
    if (ctx->caches[i].rlen)
      (*ctx->freef)(ctx->caches[i].raddress,ctx->hint);
  }
  
  (*ctx->freef) (ctx->caches,ctx->hint); 
  (*ctx->freef) (ctx,ctx->hint);

  return 0; /* FIXME */
}

int tmpcache_includecache(void *_ctx,const char *waddr,const int wlen,
			  const char *raddr,const int rlen)
{
  ctx_t *ctx = (ctx_t *)_ctx;

  /* find a free cache : */
  int i;
  address_t *addr = NULL;
  for (i=0; i < ctx->numof; i++) {
    if (ctx->caches[i].wlen == 0 && ctx->caches[i].rlen == 0) {
      addr = &ctx->caches[i];
      break;
    }
  }

  if (addr == NULL) { /* rebuild caches */
    int old_numof = ctx->numof;
    ctx->caches = allocaddresses(ctx->caches,old_numof,(old_numof * 2),
				 ctx->mallocf,ctx->freef,ctx->hint);

    addr = &ctx->caches[old_numof];
    ctx->numof = (old_numof * 2);
    /* TODO: check for bad alloc! */
  }

  if (wlen) {

    /* TODO: parse for valid address */
    addr->waddress = (char *)ctx->mallocf(wlen,ctx->hint);
    assert (addr->waddress); /* FIXME */
    memcpy(addr->waddress,waddr,wlen);
    addr->wlen = wlen;
    addr->whash = tc_hash(addr->waddress,addr->wlen);
  }

  if (rlen) {

    /* TODO: parse for valid address or does xs take care of that? */
    addr->raddress = (char *)ctx->mallocf(rlen,ctx->hint);
    assert (addr->raddress); /* FIXME */
    memcpy(addr->raddress,raddr,rlen);
    addr->rlen = rlen;
    addr->rhash = tc_hash(addr->raddress,addr->rlen);
  }

  return 0;
}

int tmpcache_connect (void *_ctx)
{
  ctx_t *ctx = (ctx_t *)_ctx;
  
  /* FIXME: check that xs is init! */
  /* FIXME: check if already connected */

  int i;
  int connects = 0;
  int writes = 0;
  int reads = 0;
  address_t *addr = NULL;
  for (i=0;i < ctx->numof;i++) {

    addr = &ctx->caches[i];
    if (!addr->wlen && !addr->rlen) 
      continue;

    if (addr->wlen) {

      addr->write = xs_socket(ctx->xsctx,XS_PUSH);
      assert (addr->write); /* FIXME */
      
      addr->winterface = xs_connect(addr->write,addr->waddress);
      assert (addr->winterface != -1); /* FIXME */
      
      writes ++;
      connects ++;
    }

    if (addr->rlen) {

      addr->read = xs_socket(ctx->xsctx,XS_REQ);
      assert (addr->read); /* FIXME */

      addr->rinterface = xs_connect(addr->read,addr->raddress);
      assert (addr->rinterface != -1); /* FIXME */
      
      reads ++;
      connects ++;
    }
  }

  if (reads) {

    ctx->readhashes = (int *)(*ctx->mallocf)(sizeof(int) * reads,ctx->hint);
    assert(ctx->readhashes); /* FIXME */
    ctx->numof_reads = reads;
    reads = 0;
  }

  if (writes) {

    ctx->writehashes = (int *)(*ctx->mallocf)(sizeof(int) * writes,ctx->hint);
    assert(ctx->writehashes); /* FIXME */
    ctx->numof_writes = writes;
    writes = 0;
  }

  for (i=0;i < ctx->numof; i++) {

    addr = &ctx->caches[i];
    if (addr->rhash != 0) {
      ctx->readhashes[reads] = addr->rhash;
      reads++;
    }

    if (addr->whash != 0) {
      ctx->writehashes[writes] = addr->whash;
      writes++;
    }
  }

   
  return connects;
}

int tmpcache_disconnect (void *_ctx) 
{
  ctx_t *ctx = (ctx_t *)_ctx;

  int i,r;
  address_t *addr = NULL;
  int disconnects = 0;
  for (i=0;i < ctx->numof; i++) {

    addr = &ctx->caches[i];
    if (!addr->read && !addr->write)
      continue;

    if (addr->write) {
      
      r = xs_shutdown(addr->write,addr->winterface);
      assert (r == 0); /* FIXME */
      r = xs_close(addr->write);
      disconnects ++;
    }

    if (addr->read) {

      r = xs_shutdown(addr->read,addr->rinterface);
      assert (r == 0); /* FIXME */
      r = xs_close(addr->read);
      disconnects ++;
    }
  }

  if (ctx->readhashes)
    (*ctx->freef)(ctx->readhashes,ctx->hint);

  if (ctx->writehashes)
    (*ctx->freef)(ctx->writehashes,ctx->hint);

  return disconnects;
}

int tmpcache_write  (void *ctx,const char *key, const int klen, void *data,int dlen)
{
  return -1;
}

int tmpcache_read   (void *ctx,const char *key, const int klen, void *buffer, int blen) 
{
  return -1;
}

int tmpcache_delete (void *ctx,const char *key, const int klen)
{
  return -1;
}
