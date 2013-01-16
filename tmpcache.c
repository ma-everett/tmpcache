
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

int tc_choosecache (const char *key,const int klen,int *cache_hashes,int numof,void *hint) {
  
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
  void * rhint;
  tmpcache_choosef writef;
  void * whint;
  tmpcache_choosef deletef;
  void * dhint;

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
  return tmpcache_custom (tc_malloc,tc_free,NULL);
}

void * tmpcache_custom (tmpcache_mallocf mallocf,tmpcache_freef freef, void *hint) 
{
  
  ctx_t *ctx = (ctx_t *)(*mallocf) (sizeof(ctx_t),hint); /* FIXME, check mallocf first! */
  assert (ctx); /* FIXME */
 
  ctx->mallocf = (mallocf) ? mallocf : tc_malloc;
  ctx->freef = (freef) ? freef : tc_free;
  ctx->hint = hint;
  ctx->readf =  tc_choosecache;
  ctx->writef =  tc_choosecache;
  ctx->deletef =  tc_choosecache;
  ctx->rhint = NULL;
  ctx->whint = NULL;
  ctx->dhint = NULL;
  ctx->xsctx = NULL;

  ctx->caches = allocaddresses(NULL,0,DEFAULT_CACHES,ctx->mallocf,ctx->freef,ctx->hint);
  ctx->numof = DEFAULT_CACHES;

  ctx->readhashes = NULL;
  ctx->numof_reads = 0;

  ctx->writehashes = NULL;
  ctx->numof_writes = 0;

  return (void *)ctx;
}

int tmpcache_option (void *_ctx,const int option,tmpcache_choosef f, void *hint) 
{
  ctx_t *ctx = (ctx_t *)_ctx;
  
  switch(option) {
  case TMPCACHE_READ:
    
    ctx->readf = (f) ? f : tc_choosecache;
    ctx->rhint = hint;
    break;
  case TMPCACHE_WRITE:

    ctx->writef = (f) ? f : tc_choosecache;
    ctx->whint = hint;
    break;

  case TMPCACHE_DELETE:

    ctx->deletef = (f) ? f : tc_choosecache;
    ctx->dhint = hint;
    break;

  default:
    return -1;
    break;
  }


  return (f) ? 1 : 0;
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
  /*assert (r == 0);*/ /* FIXME */
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

/* ask the choose function for the cache to use, then do the operation on the cache */

int tmpcache_write  (void *_ctx,const char *key, const int klen, void *data,int dlen)
{
  if (dlen <= 0)
    return 0;

  ctx_t *ctx = (ctx_t *)_ctx;
  int hash = (*ctx->writef)(key,klen,ctx->readhashes,ctx->numof_reads,ctx->rhint);  

  /* find the cache : */
  int i;
  address_t *addr = NULL;
  for (i=0; i < ctx->numof;i++) {
    addr = &ctx->caches[i];
    if (addr->rhash == hash)
      break;
  }

  if (addr == NULL)
    return -1;

  void *_data = (*ctx->mallocf)(klen,ctx->hint);
  memcpy(_data,key,klen);
  
  xs_msg_t msg_ident;
  xs_msg_init_data(&msg_ident,_data,klen,ctx->freef,ctx->hint);
  
  int r;
  r = xs_sendmsg(addr->write,&msg_ident,XS_SNDMORE);
  assert (r != -1); /* FIXME */

  _data = (*ctx->mallocf)(dlen,ctx->hint);
  memcpy(_data,data,dlen);

  xs_msg_t msg_part;
  xs_msg_init_data(&msg_part,_data,dlen,ctx->freef,ctx->hint);

  r = xs_sendmsg(addr->write,&msg_part,0);
  assert (r != -1); /* FIXME */

  return dlen;
}


int tmpcache_read   (void *_ctx,const char *key, const int klen, void *buffer, int blen) 
{
  /* stalling version of read, TODO add lazy pirate pattern here */

  ctx_t *ctx = (ctx_t *)_ctx;
  int hash = (*ctx->readf)(key,klen,ctx->readhashes,ctx->numof_reads,ctx->rhint);  

  /* find the cache : */
  int i;
  address_t *addr = NULL;
  for (i=0; i < ctx->numof;i++) {
    addr = &ctx->caches[i];
    if (addr->rhash == hash)
      break;
  }

  if (addr == NULL)
    return -1;

  void *data = (*ctx->mallocf)(klen,ctx->hint);
  memcpy(data,key,klen);
  
  xs_msg_t msg_ident;
  xs_msg_init_data(&msg_ident,data,klen,ctx->freef,ctx->hint);
  
  int r;
  r = xs_sendmsg(addr->read,&msg_ident,0);
  assert (r != -1); /* FIXME */

  r = xs_recvmsg(addr->read,&msg_ident,0);
  assert (r != -1); /* FIXME */

  xs_msg_t msg_data;
  xs_msg_init(&msg_data);
  r = xs_recvmsg(addr->read,&msg_data,0);
  assert (r != -1); /* FIXME */

  int size = xs_msg_size(&msg_data);
  memcpy(xs_msg_data(&msg_data),buffer,(size <= blen) ? size : blen);

  xs_msg_close(&msg_ident);
  xs_msg_close(&msg_data);

  return (size <= blen) ? size : blen;
}

int tmpcache_delete (void *_ctx,const char *key, const int klen)
{
  ctx_t *ctx = (ctx_t *)_ctx;
  int hash = (*ctx->deletef)(key,klen,ctx->writehashes,ctx->numof_writes,ctx->dhint);

  int i;
  address_t *addr = NULL;
  for (i=0;i < ctx->numof;i++) {
    addr = &ctx->caches[i];
    if (addr->whash == hash)
      break;
  }

  if (addr == NULL)
    return -1;
  
  void *data = (*ctx->mallocf)(klen,ctx->hint);
  memcpy(data,key,klen);

  xs_msg_t msg_ident;
  xs_msg_init_data(&msg_ident,data,klen,ctx->freef,ctx->hint);

  int r;
  r = xs_sendmsg(addr->write,&msg_ident,XS_SNDMORE);
  assert( r != -1); /*FIXME */
  
  xs_msg_t msg_part;
  xs_msg_init (&msg_part);

  r = xs_sendmsg(addr->write,&msg_part,0);
  assert( r != -1); /*FIXME */

  return 0;
}



/* this is an example of lazy pirate pattern with timeout */
int lazypirate_read (void *_ctx,const char *key,const int klen,void *data,int dlen)
{
  ctx_t *ctx = (ctx_t *)_ctx;
  int hash = (*ctx->readf)(key,klen,ctx->readhashes,ctx->numof_reads,ctx->rhint);  

  /* find the cache : */
  int i;
  address_t *addr = NULL;
  for (i=0; i < ctx->numof;i++) {
    addr = &ctx->caches[i];
    if (addr->rhash == hash)
      break;
  }

  if (addr == NULL)
    return -1;

  /* build then send msg for read here */
  
  xs_pollitem_t pitems[1];
  pitems[0].socket = addr->read;
  pitems[0].events = XS_POLLIN;

  int count = 0;

  /* wait for timeout */

  for (;;) {

    count = xs_poll (pitems,1,(1000 * 10));
    
    if (pitems[0].revents & XS_POLLIN) {
      break;
    }
  }

  /* if failure, shutdown connection and retry? or fail */

  /* on success, read data back ... done */


  return 0;
}
