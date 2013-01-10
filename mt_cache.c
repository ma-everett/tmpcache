
/* tmpcache/mt_cache.c */
/* multi-threaded rewrite of tmpcache 0 */

/* TODO: 
 *      + apply pikestyle to the code
 *      + (!!) syslog debug logging
 *      + document everything as it gets tidy and settled
 *      + (!) add a heap of client unit tests to stress test the current implementation
 *      d (!) ensure st_cache and mt_cache compile and run correctly
 *      + add release memory function on write
 *      d (!) rearrange code so that main is at the bottom with no forward dec'ls 
 */
/*
#define CACHE_MEM_DEBUG
#define CACHE_MT
#define CACHE_SYSLOG
#define CACHE_DEBUG
#define CACHE_BLANK 
*/

#if defined CACHE_MT
#define VERSION "tmpcache 1 mt"
#else
#define VERSION "tmpcache 1"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <time.h>

extern int nanosleep(const struct timespec *, struct timespec *);

#include <assert.h>
#include <argp.h>

#if defined CACHE_SYSLOG
#include <syslog.h>
#endif

#include <xs/xs.h>
#include "PMurHash.h"

#define HASH_SEED 101
#define cache_hash(s,l) PMurHash32(HASH_SEED,(s),(l))

int u_term = 0; /* SIGTERM */
/*
void signal_handler (int signo);
void catch_signals (void);
*/
/* arg parsing : */

const char *argp_program_version = VERSION;
const char *argp_program_bug_address = "<..>";

static char doc[] = 
  "tmpcache, a memory based cache using message passing";

static char args_doc[] = ""; /* FIXME ? */

static struct argp_option argp_options[] = {
  {"memory", 'm', "64MB", 0, "Total memory"},
  {"size",   's', "1MB",0, "Max storage size per data"},
  {"write",  'w', "ADDR",0,"Write address for cache, example: tcp://127.0.0.1:5678"},
  {"read",   'r', "ADDR",0,"Read address for cache, example: tcp://127.0.0.1:5679"},
  {0}
};

typedef struct arguments {

  long long memory;      /* total memory size in bytes */
  long long size;        /* maximum data size in bytes */
  char *waddress;        /* xs network address for write|delete operations */
  char *raddress;        /* xs network address for read operations */ 

} arguments_t;

static error_t parse_options (int key, char *arg,struct argp_state *state);
static struct argp argp = {argp_options,parse_options,args_doc,doc};

/* ------------------- */

/* new cache : */

typedef struct {

  int hash;
  xs_msg_t data;

} cache_item_t;

typedef struct {

  cache_item_t *items;
  int numof;

} cache_itemarray_t;

#if defined CACHE_MT
typedef struct {

  int hash;
  cache_item_t *item;

} cache_tableitem_t;

typedef struct {

  cache_tableitem_t *items;
  int numof;

} cache_table_t;
#endif

typedef struct {

  cache_itemarray_t ring;
  int current;

  long long maxsize;
  
#if defined CACHE_MT

  cache_table_t *read;
  cache_table_t *middleground;
  cache_table_t *write;

  cache_item_t *witem;
#endif
} cache_t;


#if defined CACHE_BLANK
int read_then_send_blank(void *sck)
{
  xs_msg_t msg_from;
  xs_msg_init (&msg_from);

  int r = xs_recvmsg(sck,&msg_from,0);
  assert(r != -1);

  xs_msg_t msg_blank;
  xs_msg_init (&msg_blank);
  r = xs_recvmsg(sck,&msg_blank,0);
  assert(r != -1);
  
  xs_msg_t msg_ident;
  xs_msg_init (&msg_ident);
  xs_recvmsg(sck,&msg_ident,0);

  xs_sendmsg(sck,&msg_from,XS_SNDMORE);
  xs_sendmsg(sck,&msg_blank,XS_SNDMORE);
  
  xs_sendmsg(sck,&msg_ident,XS_SNDMORE);

  xs_msg_init (&msg_blank);
  xs_sendmsg(sck,&msg_blank,0);

  return 1;
}
#endif



int read_then_send(void *sck,cache_t *cache)
{
#if defined CACHE_DEBUG
  printf("%s\n",__FUNCTION__);
#endif
  int r,ret; 
  int hash = 0;
  int slot,i;
  long long size;
  char buffer[256];
 
  /* read the from frame : */
  xs_msg_t msg_from;
  xs_msg_init (&msg_from);

  r = xs_recvmsg(sck,&msg_from,0);
  assert(r != -1);
  
  /* read the blank frame : */
  xs_msg_t msg_blank;
  xs_msg_init (&msg_blank);

  r = xs_recvmsg(sck,&msg_blank,0);
  assert(r != -1);  

  /* read & check the identity : */
  xs_msg_t msg_ident;
  xs_msg_init (&msg_ident);

  r = xs_recvmsg(sck,&msg_ident,0); 
  assert(r != -1);

  size = xs_msg_size(&msg_ident);
  if (size <= 0 || size > 256) {

    /* err - improper identity size */
    ret = 0; 
    goto err;
  }

  char * identity = (char *)xs_msg_data(&msg_ident);
  /* hash the identity : */
  hash = cache_hash(identity,size);

  memcpy(&buffer[0],xs_msg_data(&msg_ident),size);

  /* find the item in the cache : */

  /* start at the current write position and work backwards 
   * MAYBE there is a more elegent way of doing this?
   */
  

#if defined CACHE_MT
#if defined CACHE_DEBUG
  printf("%s - looking for slot of hash %d\n",__FUNCTION__,hash);
#endif
  slot = -1;
  for (i = 0; i < cache->read->numof; i++) {
    
    if (cache->read->items[i].hash == hash) {
      slot = i;
      break;
    }
  }

#else

  int cpos = cache->current;
  slot = -1;
  
  for(i = cpos; i >= 0; i--) {
    
    cache_item_t *item = &cache->ring.items[i];
    
    if (item->hash == hash) {
      slot = i;
      break;
    }
  }
  
  /* go backwards if still unfound */
  if (slot == -1) {
    for(i = (cache->ring.numof - 1); i >= cpos; i--) {
      
      cache_item_t *item = &cache->ring.items[i];

      if (item->hash == hash) {
	slot = i;
	break;
      }
    }
  }

#endif /* CACHE_MT */

  xs_msg_t msg_part;
  xs_msg_init(&msg_part);
  r = xs_sendmsg(sck,&msg_from,XS_SNDMORE);
  assert(r != -1);
  r = xs_sendmsg(sck,&msg_blank,XS_SNDMORE);
  assert(r != -1);
  r = xs_sendmsg(sck,&msg_ident,XS_SNDMORE);
  assert(r != -1);
  

  if (slot != -1) {

    /* make a copy and send */    
#if defined CACHE_MT   
    xs_msg_copy(&msg_part,&cache->read->items[slot].item->data);
#else 
    xs_msg_copy(&msg_part,&cache->ring.items[slot].data);
#endif  

    size = xs_msg_size(&msg_part);
     
    r = xs_sendmsg(sck,&msg_part,0);
    assert(r != -1); 

#if defined SYSLOG
    syslog(LOG_DEBUG,"read %s, sent %dKb",buffer[0],size / 1024);
#endif

    goto early;
  }

#if defined SYSLOG
    syslog(LOG_DEBUG,"read %s, miss",buffer[0]);
#endif

  /* send a blank */
  r = xs_sendmsg(sck,&msg_part,0);
  assert(r != -1);
  

 early:
  xs_msg_close(&msg_part);
  ret = 1; /* for successful send */

 err:

  xs_msg_close (&msg_from);
  xs_msg_close (&msg_blank);
  xs_msg_close (&msg_ident);

  return ret; /* 1 for valid message, 0 for non valid message */
}

/* WRITE|DELETE */
#if defined CACHE_MT
int writedelete_into(void *sck,void *csck,cache_t *cache)
#else
int writedelete_into(void *sck,cache_t *cache)
#endif
{
#if defined CACHE_DEBUG
  printf("%s\n",__FUNCTION__);
#endif
  int r = 0;
  long long size = 0;
  int ret = 0;
  int i=0;

#if defined CACHE_MT
  assert(cache->witem);
#endif

  xs_msg_t msg_ident;
  xs_msg_init(&msg_ident);
  
  r = xs_recvmsg(sck,&msg_ident,0);
  assert(r != -1);
  
  /* hash the ident */
  size = xs_msg_size(&msg_ident);
  
  if (size <= 0 || size > 256) {

#if defined CACHE_DEBUG
    printf("%s identity size incorrect - %lld\n",__FUNCTION__,size);
#endif    
    ret = -1;
    goto err;
  }

  char *identity = xs_msg_data(&msg_ident);
  int hash = cache_hash(identity,size);

  xs_msg_t msg_part;
  xs_msg_init(&msg_part);

  /* TODO: check for data part first */

  /* check for data, if size = 0 then delete command */
  r = xs_recvmsg(sck,&msg_part,0);
  assert(r != -1);

  size = xs_msg_size(&msg_part);

  if (size > cache->maxsize) {
#if defined CACHE_DEBUG
    printf("\t%s - data too large for cache!\n",__FUNCTION__);
#endif    
    ret = -1;
    goto early;
  }

  if (size == 0) { /* DELETE */
#if defined CACHE_DEBUG
    printf("\t%s - performing a delete request\n",__FUNCTION__);
#endif

#if defined CACHE_MT

    int numto_delete = 0;
    for (i=0; i < cache->write->numof; i++) {

      cache_tableitem_t *item = &cache->write->items[i];
      if (item->hash == hash) {
	item->hash = 0;
	numto_delete ++;
      }
    }

    if (numto_delete) { /* if there is something to actually update */

      cache->middleground = cache->write;
#if defined CACHE_DEBUG
      printf("\t%s - swapping\n",__FUNCTION__);
#endif
      xs_msg_t msg_change;
      xs_msg_init(&msg_change);
      memcpy(xs_msg_data(&msg_change),"ABC",3);
      
      r = xs_sendmsg(csck,&msg_change,0);
      assert(r != -1);
      
      r = xs_recvmsg(csck,&msg_change,0);
      assert(r != -1);
      
      xs_msg_close(&msg_change);
      
      assert(cache->middleground);
      cache->write = cache->middleground;
      
      for (i=0; i < cache->write->numof; i++) {
	
	cache_tableitem_t *item = &cache->write->items[i];
	if (item->hash == hash) {
	  item->hash = 0;
	  item->item->hash = 0;
	  xs_msg_close(&item->item->data);
	  xs_msg_init(&item->item->data);
	}
      } 
    }   
#else
      
      for (i=0; i < cache->ring.numof; i++) {

      cache_item_t *item = &cache->ring.items[i];
      if (item->hash == hash) {
	item->hash = 0;
	xs_msg_close(&item->data);
	xs_msg_init(&item->data);
      }
    }
#endif  
    ret = -1;
    goto early;
  }
  
  /* TODO: check that we have enough space first */

  /* WRITE */
  
 

#if defined CACHE_MT

  cache->witem->hash = hash;
  xs_msg_copy(&cache->witem->data,&msg_part);
 
  cache->write->items[cache->current].hash = hash;
  cache->write->items[cache->current].item = cache->witem;
 
  cache->middleground = cache->write;
  xs_msg_t msg_change;
  xs_msg_init(&msg_change);
  memcpy(xs_msg_data(&msg_change),"ABC",3);
  
  r = xs_sendmsg(csck,&msg_change,0);
  assert(r != -1);

#if defined CACHE_DEBUG
  printf("%s - awaiting reply\n",__FUNCTION__);
#endif
  r = xs_recvmsg(csck,&msg_change,0);
  assert(r != -1);

  assert(cache->middleground);
  cache->write = cache->middleground;
#if defined CACHE_DEBUG
  printf("%s - swapping out item for later use\n",__FUNCTION__);
#endif
  cache->write->items[cache->current].hash = 0; 
  cache->witem = cache->write->items[cache->current].item;

  assert(cache->witem);

  cache->witem->hash = 0;

#if defined CACHE_DEBUG
  printf("%s - swapped changes, ready for next write\n",__FUNCTION__);
#endif

#else

  cache->ring.items[cache->current].hash = hash;
  xs_msg_copy(&cache->ring.items[cache->current].data,&msg_part); 

#endif

 cache->current = (cache->current + 1 < cache->ring.numof) ? cache->current + 1 : 0;

 ret = 1;  

#if defined CACHE_DEBUG
 printf("%s - successful write of %lldKb\n",__FUNCTION__,size / 1024);
#endif

 early:
 xs_msg_close(&msg_part);

 err:
 xs_msg_close(&msg_ident);


 return ret;
}


typedef struct {

  arguments_t *options;
  int running;
  void *ctx;

  cache_t *cache;
  cache_item_t *writable;

  int timeout;

} mt_kernel_t;

#if defined CACHE_MT
void * mt_read_cache (void *arg) {

  struct timespec req,rem;
  req.tv_sec = 2;
  req.tv_nsec = 0;
  nanosleep(&req,&rem);

  printf("> %s start\n",__FUNCTION__);

  mt_kernel_t *kernel = (mt_kernel_t *)arg;
  
  void * read_sck = xs_socket(kernel->ctx,XS_XREP);
  assert (read_sck);

  int r = xs_bind(read_sck,kernel->options->raddress);
  assert (r != -1);
  int interface = r;
  printf (">> %s bind @ %s\n",__FUNCTION__,kernel->options->raddress);
 
  void * change_sck = xs_socket(kernel->ctx,XS_PAIR);
  assert (change_sck);
  r = xs_connect(change_sck,"inproc://changes");
  if (r == -1) {
    printf("%s change_sck connection error : %s\n",__FUNCTION__,xs_strerror(xs_errno()));
    abort();
  }

  printf (">> %s bind internal @ inproc://changes\n",__FUNCTION__);

  xs_pollitem_t pitems[2];
  pitems[0].socket = read_sck;
  pitems[0].events = XS_POLLIN;
  pitems[1].socket = change_sck;
  pitems[1].events = XS_POLLIN;
  int count = 0;
  int successful_reads = 0;

  for (;;) {
    
    if (count == 0)
      count = xs_poll (pitems,2,kernel->timeout);

    if (kernel->running == 0) {
      break;
    }

    if (count == 0)
      continue;

    if (pitems[1].revents & XS_POLLIN) {

      xs_msg_t msg_part;
      xs_msg_init(&msg_part);

#if defined CACHE_DEBUG
      printf("%s - updating from changes\n",__FUNCTION__);
#endif
      r = xs_recvmsg(change_sck,&msg_part,0);
      assert(r != -1);

      assert(kernel->cache->middleground);

    
      cache_table_t *read = kernel->cache->read;
      kernel->cache->read = kernel->cache->middleground;
     
      kernel->cache->middleground = read;

      r = xs_sendmsg(change_sck,&msg_part,0);
      assert(r != -1);

      xs_msg_close(&msg_part);      
#if defined CACHE_DEBUG
      printf("%s - successful update from changes\n",__FUNCTION__);
#endif      
      count--;
    }


    if (pitems[0].revents & XS_POLLIN) {
      /* else parse read message : */
      successful_reads += read_then_send(read_sck,kernel->cache);    
      count--;
    }
  }
  
  printf(">> %s shutdown @ %s\n",__FUNCTION__,kernel->options->raddress);
  r = xs_shutdown (read_sck,interface);
  assert (r != -1);
  r = xs_close (read_sck);
  assert (r != -1);

  r = xs_close (change_sck);
  assert (r != -1);

  printf("> %s term\n",__FUNCTION__);
  return NULL;
}



void * mt_write_cache (void *arg) 
{

  printf("> %s start\n",__FUNCTION__);

  mt_kernel_t *kernel = (mt_kernel_t *)arg;

  /* setup the write address : */
  void * write_sck = xs_socket(kernel->ctx,XS_PULL);
  assert (write_sck);

  int r = xs_bind(write_sck,kernel->options->waddress);
  assert (r != -1);

  int interface = r;

  printf(">> %s bind @ %s\n",__FUNCTION__,kernel->options->waddress);

  void *change_sck = xs_socket(kernel->ctx,XS_PAIR);
  assert(change_sck);

  r = xs_bind(change_sck,"inproc://changes");
  assert(r != -1);

  xs_pollitem_t pitems[1];
  pitems[0].socket = write_sck;
  pitems[0].events = XS_POLLIN;

  int count = 0;

  for (;;) {

    if (count == 0)
      count = xs_poll (pitems,1,kernel->timeout);

    if (kernel->running == 0) {
      break;
    }

    if (pitems[0].revents & XS_POLLIN) {
    
      /* else parse message */
      writedelete_into(write_sck,change_sck,kernel->cache);
      count--;   
    }
  }

  printf(">> %s shutdown @ %s\n",__FUNCTION__,kernel->options->waddress);
  r = xs_shutdown (write_sck,interface);
  assert (r != -1);
  r = xs_close (write_sck);
  assert (r != -1);
  r = xs_close (change_sck);
  assert (r != -1);  

  printf("> %s term\n",__FUNCTION__);
  return NULL;
}
#endif /* CACHE_MT */

/* cache_alloc : 
 */
#if defined CACHE_MEM_DEBUG
void * cache_alloc(void *p,size_t size,const char *src,const int line)
#else
void * cache_alloc(void *p,size_t size)
#endif

{
  if (size == 0) {
    
#if defined CACHE_MEM_DEBUG
    printf("free  %d\t\t\t@%s:%d\n",(int)p,src,line);
#endif
    
    free(p);
    return NULL;
  }

  void *m = malloc(size);
#if defined CACHE_MEM_DEBUG
  printf("malloc %d\t%db\t@%s:%d\n",(int)m,size,src,line);
#endif

  return m;
}

#if defined CACHE_MEM_DEBUG
#define cache_malloc(s) cache_alloc(NULL,(s),__FILE__,__LINE__)
#define cache_free(p) cache_alloc((p),0,__FILE__,__LINE__)
#else
#define cache_malloc(s) cache_alloc(NULL,(s))
#define cache_free(p) cache_alloc((p),0)
#endif



/* signal handling */

void signal_handler(int signo)
{
  if (signo == SIGINT || signo == SIGTERM)
    u_term = 1;
}

void catch_signals(void) {

  signal(SIGINT,signal_handler);
  signal(SIGTERM,signal_handler);
}

#if defined CACHE_MT
int mt_cache (arguments_t *options,cache_t *cache) 
{
  printf("running multi-threaded cache\n");

  catch_signals();

  /* start up crossroads i/o */
  mt_kernel_t kernel;
  kernel.options = options;
  kernel.running = 1;
  kernel.ctx = xs_init( );
  assert (kernel.ctx);

  kernel.cache = cache; 
  kernel.timeout = (1000 * 5); /* 1 second */

 
  kernel.writable = &kernel.cache->ring.items[ (kernel.cache->ring.numof) ];
 
  /* launch 2 threads one for READ and one for WRITE
   * operations, this main thread will catch any CTRL
   * operations..
   */  

  pthread_t read_t, write_t;
  
  void *mt_read_cache(void *);
  void *mt_write_cache(void *);
  
  if ( pthread_create(&read_t,NULL,mt_read_cache,(void *)&kernel) != 0) {
    printf("pthread_create error\n");
    abort();
  }

  if ( pthread_create(&write_t,NULL,mt_write_cache,(void *)&kernel) != 0) {
    printf("pthread_create error\n");
    abort();
  }

  /* multi-threaded cache */
  struct timespec req,rem;
  req.tv_sec = 1;
  req.tv_nsec = 0;

  for(;;) {

    if (u_term == 1) {
      printf("user interrupt\n");
      break;
    }

    nanosleep(&req,&rem);
  }

  kernel.running = 0;

  pthread_join(read_t,NULL);
  pthread_join(write_t,NULL); 

 
  xs_term(kernel.ctx);

  return 0;
}  
#endif /*CACHE_MT*/

#ifndef CACHE_MT
int st_cache (arguments_t *options,cache_t *cache) 
{
  printf("running single-threaded cache\n");

  /* single-threaded cache */
  catch_signals();
   
  void * ctx = xs_init();
  assert(ctx);

  void * write_sck = xs_socket(ctx,XS_PULL);
  assert (write_sck);

  int r = xs_bind(write_sck,options->waddress);
  assert (r != -1);

  int interface = r;

  printf(">> %s bind @ %s\n",__FUNCTION__,options->waddress);

  void * read_sck = xs_socket(ctx,XS_XREP);
 
  assert (read_sck);

  r = xs_bind(read_sck,options->raddress);
  assert (r != -1);

  printf(">> %s bind @ %s\n",__FUNCTION__,options->raddress);
  
  /*
  struct timespec req,rem;
  req.tv_sec = 1;
  req.tv_nsec = 0;
  */

  xs_pollitem_t pitems[2];
  pitems[0].socket = read_sck;
  pitems[0].events = XS_POLLIN;
  pitems[1].socket = write_sck;
  pitems[1].events = XS_POLLIN;

  int count = 0;
  int successful_reads = 0;

  for(;;) {

    if (count == 0)
      count = xs_poll(pitems,2,(1000 * 5));


    if (u_term == 1) {
      printf("user interrupt\n");
      break;
    }

    if (pitems[0].revents & XS_POLLIN) {

#if defined CACHE_BLANK
      successful_reads += read_then_send_blank(read_sck);
#else
      successful_reads += read_then_send(read_sck,cache);   
#endif      
      count--;
    }

    if (pitems[1].revents & XS_POLLIN) {

      writedelete_into(write_sck,cache);
      count--;
    }
    
    /*nanosleep(&req,&rem);*/
  }

  r = xs_shutdown(write_sck,interface);
  assert(r != -1);

  r = xs_close(write_sck);
  assert(r != -1);

  r = xs_close(read_sck);
  assert(r != -1);

  r = xs_term(ctx);
  assert(r != -1);
  
  printf(">> %s - %d successful reads\n",__FUNCTION__,successful_reads);


  return 0;
}
#endif

long long parse_str2bytes (const char *str) 
{
  char buffer[256];
  memset(&buffer[0],'\0',256);

  int n;
  int r = sscanf(str,"%d%s",&n,&buffer[0]);
  assert(r == 2);
    
  char *bp = &buffer[0];
  while ( *bp != '\0') {
    
    unsigned char c = toupper ((unsigned char)*bp);
    
    switch(c) {
    case 'B':    
      return n * (long long)1024;
      break;
    case 'M':
      return n * (long long)(1024 * 1024);
      break;    
    case 'G':
      return n * (long long)((1024 * 1024) * 1024);
      break;    
    default:
 
      break;
    }

    ++bp;	   
  }  
  
  return n * 1024;
}



static error_t parse_options (int key, char *arg,struct argp_state *state) 
{
  arguments_t *arguments = state->input;

  switch (key)
    {
    case 'm' :
      arguments->memory = arg ? parse_str2bytes(arg) : arguments->memory;
      break;
    case 's' :
      arguments->size = arg ? parse_str2bytes(arg) : arguments->size;
      break;
    case 'w' :
      arguments->waddress = arg;
      break;
    case 'r' :
      arguments->raddress = arg;
      break;
   
    case ARGP_KEY_ARG:
      if (state->arg_num > 1)
	argp_usage(state);

      break;
      
    case ARGP_KEY_END:
      break;

    default:
      return ARGP_ERR_UNKNOWN;
    }

  return 0;
}


int main (int argc,char **argv) {
  
  arguments_t options;
  options.memory = 64 * (1024 * 1024); 
  options.size = 1024 * 1024;
  options.waddress = NULL;
  options.raddress = NULL;

  argp_parse (&argp,argc,argv,0,0,&options);

  if (options.memory < options.size) {
    printf("error - total memory less than maximum data size\n");
    abort();
  }

  if (!strlen(options.waddress) && !strlen(options.raddress)) { /* pointless cache */
    printf("error - %s --help for details\n",argv[0]);
    abort();
  }

  /* HORRIBLE: */
  int size_of_item_ring = (options.memory / options.size);
  printf("> %d items in ring\n",size_of_item_ring); 

  cache_t cache;
  cache.ring.items = (cache_item_t *)cache_malloc(sizeof(cache_item_t) * size_of_item_ring + 1);
  cache.ring.numof = size_of_item_ring;
  cache.current = 0;
  int i;
  for (i=0; i < cache.ring.numof + 1; i++) {

    cache_item_t *item = &cache.ring.items[i];
    item->hash = 0;
    xs_msg_init(&item->data);   
  }

#if defined CACHE_MT

  cache.read = (cache_table_t *)cache_malloc(sizeof(cache_table_t));
  cache.read->items = (cache_tableitem_t *)cache_malloc(sizeof(cache_tableitem_t) * cache.ring.numof);
  cache.read->numof = cache.ring.numof;

  cache.write = (cache_table_t *)cache_malloc(sizeof(cache_table_t));
  cache.write->items = (cache_tableitem_t *)cache_malloc(sizeof(cache_tableitem_t) * cache.ring.numof);
  cache.write->numof = cache.ring.numof;

  for (i=0; i < cache.read->numof; i++) {
    cache.read->items[i].hash = 0;
    cache.read->items[i].item = &cache.ring.items[i];
    cache.write->items[i].hash = 0;
    cache.write->items[i].item = &cache.ring.items[i];
  }

  cache.witem = &cache.ring.items[(cache.ring.numof)];

  mt_cache(&options,&cache);

  cache_free (cache.read->items);
  cache_free (cache.read);
  cache_free (cache.write->items);
  cache_free (cache.write);

#else
  st_cache(&options,&cache);
#endif
 
  for(i=0; i < cache.ring.numof + 1; i++) {
    
    cache_item_t *item = &cache.ring.items[i];
    xs_msg_close(&item->data);
  }

  cache_free (cache.ring.items);
  
  exit(0);
}
