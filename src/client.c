
/* unit/stress tests:
 *
 *  + multiple threaded reading
 *  + multiple threaded writing
 *  + multiple threaded deleting
 */

#include "bstrlib/bstrlib.h"
#include "bstrlib/bsafe.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>

#include "tmpcache.h"

#include <xs/xs.h>

#define ADDRESS "ipc:///mnt/git/xs_cache/run/read.feed1.ipc"
#define ADDRESS2 "ipc:///mnt/git/xs_cache/run/write.feed1.ipc"


typedef struct {

  int watermark;
  int free;
  int malloc;
  int choose;

} hint_t;

void * custom_malloc(int size, void * _hint)
{
  printf("malloc of size %db\n",size);
  hint_t * hint = (hint_t *)_hint;
  assert(hint);
  hint->watermark += size;
  hint->malloc ++;
  return malloc (size);
}

void custom_free(void *p, void *_hint)
{
  hint_t * hint = (hint_t *)_hint;
  hint->free ++;

  free (p);
}

int custom_choose(const char *key,const int klen,int *caches,int numof,void *_hint) {

  hint_t * hint = (hint_t *)_hint;
  hint->choose++;

  int hash = tmpcache_hash(key,klen);
  
  return caches[(hash % numof)];
}



int main(void) {

  hint_t hint;
  hint.watermark = 0;
  hint.malloc = 0;
  hint.free = 0;
  hint.choose = 0;

  void * ctx = tmpcache_custom(custom_malloc,custom_free,(void *)&hint);
  assert(ctx);

  tmpcache_option(ctx,TMPCACHE_READ,custom_choose,&hint);
  tmpcache_option(ctx,TMPCACHE_WRITE,custom_choose,&hint);
  tmpcache_option(ctx,TMPCACHE_DELETE,custom_choose,&hint);

  int r = tmpcache_open(ctx);
  assert (r == 0);

  r = tmpcache_includecache (ctx,"ipc:///mnt/git/tmpcache/run/write",33,
			     "ipc:///mnt/git/tmpcache/run/read",32);
  
  assert (r == 0);

 
  
  r = tmpcache_connect(ctx);
  assert (r);

  printf("number of cache connections %d\n",r);

  /* do stuff : */
  void *buffer = malloc(1024 * 1024);
  printf("--> read\n");
  r = tmpcache_read (ctx,"foo",3,buffer,1024 * 1024);
  assert(r != -1);

  printf("--> delete\n");
  r = tmpcache_delete (ctx,"foo",3);
  assert(r != -1);

  printf("--> read\n");
  r = tmpcache_read (ctx,"foo",3,buffer,1024 * 1024);
  assert(r != -1);

  

  void *data = malloc(50);
  memset(data,'1',50);

  r = tmpcache_write (ctx,"foo",3,data,50);
  assert(r != -1);
  printf("--> write %d\n",r);

  
  
  bstring name;
  int i=0;
  for (i; i < 50; i++) {

    name = bformat("foo-%d",i);
    r = tmpcache_write (ctx,(char *)name->data,blength(name),data,50);
    printf("--> write %s %d\n",(const char *)name->data,r);
  }

  bdestroy(name);

  free (data);

  r = tmpcache_read (ctx,"foo",3,buffer,1024 * 1024);


  free (buffer);
  
  
  sleep(2);
  r = tmpcache_disconnect(ctx);
  assert (r);

  printf("number of cache disconnects %d\n",r);

  r = tmpcache_close (ctx);
  assert (r == 0);

  r = tmpcache_close(ctx);
  assert (r == 0);

  tmpcache_term(ctx);

  printf("watermark %db malloc %d free %d choose %d\n",hint.watermark,hint.malloc,hint.free,hint.choose);
  return 0;
}


