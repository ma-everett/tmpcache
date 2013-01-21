
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
#define ADDRESS3 "tcp://127.0.0.1:8081"
#define ADDRESS4 "tcp://127.0.0.1:8082"


#define PAYLOAD_NUMBER 262144

void fromxs_free (void *data,void *hint) { printf("%s called\n",__FUNCTION__);free (data); }

void send_random_data (void *sck, xs_msg_t *ident) {
  
  int r = 0;

  xs_msg_t msg_part;
  xs_msg_init (&msg_part);
  xs_msg_copy (&msg_part,ident);

  r = xs_sendmsg (sck,&msg_part,XS_SNDMORE);
  assert (r != -1);
  
  int numberof = rand() % PAYLOAD_NUMBER + 100;
  
  float *fdata = (float *)malloc (sizeof(float) * numberof);
  int i;
  for (i=0; i < numberof; i++)
    fdata[i] = 0.01f * (float)i;
    
  r = xs_msg_init_data (&msg_part,(void *)fdata,sizeof(float) * numberof,fromxs_free,NULL);
  assert( r == 0);
  r = xs_sendmsg (sck,&msg_part,0);
  assert (r != -1);
}

int read_random_data (void *sck,xs_msg_t *ident) {

  int r = 0;
  int ret = 0;

  xs_msg_t msg_part;
  xs_msg_init(&msg_part);
  xs_msg_copy(&msg_part,ident);

  r = xs_sendmsg (sck,&msg_part,0);
  assert(r != -1);

  r = xs_recvmsg (sck,&msg_part,0);
  assert(r != -1);

  r = xs_recvmsg (sck,&msg_part,0);
  assert(r != -1);

  if (xs_msg_size(&msg_part)) 
    ret = 1;

  xs_msg_close(&msg_part);
  return ret;
}


int delete_random_data (void *sck,xs_msg_t *ident) {

  int r = 0;
 
  xs_msg_t msg_part;
  xs_msg_init(&msg_part);
  xs_msg_copy(&msg_part,ident);

  r = xs_sendmsg(sck,&msg_part,XS_SNDMORE);
  assert(r != -1);
  
  xs_msg_init(&msg_part);
  r = xs_sendmsg(sck,&msg_part,0);
  assert(r != -1);

  return 1;
}



void read_test(void) {

  srand(10101);

  void *ctx = xs_init ();
  assert (ctx);

  void *request = xs_socket (ctx,XS_REQ);
  assert (request);

  int r = xs_connect (request,ADDRESS);
  assert (r != -1);

  xs_msg_t msg_ident;
  xs_msg_init (&msg_ident);
  
  int numbertotest = 100000;

  void *identity = malloc (sizeof(char) * 3);
  memcpy (identity,"foo",3);
  r = xs_msg_init_data (&msg_ident,identity,3,fromxs_free,NULL);
  assert(r == 0);

  int i;
  int misses = 0;
  int ok = 0;
  for ( i=0; i < numbertotest; i++) {
    
    int total_s = 0;
    int ret = read_random_data (request,&msg_ident);
    if (ret) {
      ok ++;
    } else {
      misses++;
    }

    //printf("\t%d/%d read - %d misses / %d ok\n",i,numbertotest,misses,ok);    
  }

  xs_msg_close(&msg_ident);
  xs_close (request);
  xs_term(ctx);
}

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


