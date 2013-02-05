
/* tmpcache/tmpcache_client.c */


#include "../config.h"
#include "utility.h"
#include "cache.h"

#include <zmq.h>
#include <argtable2.h>

struct arg_int *test;
struct arg_int *ntimes, *timeout;
struct arg_lit *verbose, *help,*blocking;
struct arg_str *key;
struct arg_file *netaddress0;
struct arg_end *end;

typedef struct {

  uint32_t numof;
  uint32_t misses;
  
  uint64_t smallest;
  uint64_t largest;

} info_t;

#define zmq_assertmsg(s,str) if ( ! (s) ) {\
  fprintf(stderr,"%s - %s\n",(str),zmq_strerror(zmq_errno()));\
  goto error; }

uint32_t blockingf (void *sock, bstring key, uint32_t tries, uint32_t timeout, void *hint)
{
  info_t *info = (info_t *)hint;

  uint32_t r = 0;
  zmq_msg_t msg_key;

  void *dkey = c_malloc (sizeof(char) * blength(key), NULL);
  memcpy (dkey,btocstr(key),blength(key));
  r = zmq_msg_init_data (&msg_key,dkey,blength(key),c_free,NULL);
  zmq_assertmsg (r != -1,"msg init data key");

  r = zmq_sendmsg (sock,&msg_key,0);
  zmq_assertmsg (r != -1,"sendmsg key");

  r = zmq_recvmsg (sock,&msg_key,0);
  zmq_assertmsg (r != -1,"recvmsg key");

  zmq_msg_t msg_part;
  zmq_msg_init (&msg_part);

  r = zmq_recvmsg (sock,&msg_part,0);
  zmq_assertmsg (r != -1,"recvmsg part");

  uint64_t size = zmq_msg_size (&msg_part);

  if (size) {
	
    if (info = NULL) {
      fputs(zmq_msg_data (&msg_part),stdout);
      fprintf(stdout,"\n");
    } else {
      
      info->largest = (size > info->largest) ? size : info->largest;
      info->smallest = (size < info->smallest) ? size : info->smallest;
    }
  } else {
    
    if (info)
      info->misses ++;
  }    

  r = zmq_msg_close (&msg_part);
  zmq_assertmsg (r != -1,"msg close part");

  r = zmq_msg_close (&msg_key);
  zmq_assertmsg (r != -1,"msg close key");
  
 error:
  return 1;
}


uint32_t lazypriatef (void *sock, bstring key, uint32_t tries, uint32_t timeout, void *hint)
{
  info_t *info = (info_t *)hint;

  uint32_t r = 0;
  zmq_msg_t msg_key;

  void *dkey = c_malloc (sizeof(char) * blength(key), NULL);
  memcpy (dkey,btocstr(key),blength(key));
  r = zmq_msg_init_data (&msg_key,dkey,blength(key),c_free,NULL);
  zmq_assertmsg (r != -1,"msg init data key");

  r = zmq_sendmsg (sock,&msg_key,0);
  zmq_assertmsg (r != -1,"sendmsg key");

  zmq_pollitem_t pitems[1];
  pitems[0].socket = sock;
  pitems[0].events = ZMQ_POLLIN;

  uint32_t i;
  for (i = 0; i < tries; i++) {

    uint32_t count = zmq_poll (pitems,1,(1000 * timeout));
    if (count) {

      r = zmq_recvmsg (sock,&msg_key,0);
      zmq_assertmsg (r != -1,"recvmsg key");

      /*FIXME: check for more */
      zmq_msg_t msg_part;
      zmq_msg_init (&msg_part);

      r = zmq_recvmsg (sock,&msg_part,0);
      zmq_assertmsg (r != -1,"recvmsg part");
   
      uint64_t size = zmq_msg_size (&msg_part);

      if (size) {

	if (info == NULL) {
	  fputs(zmq_msg_data (&msg_part),stdout);
	  fprintf(stdout,"\n");
	} else {
	  
	  info->largest = (size > info->largest) ? size : info->largest;
	  info->smallest = (size < info->smallest) ? size : info->smallest;
	}
      } else {

	if (info) 
	  info->misses ++;
      }

      r = zmq_msg_close (&msg_part);
      zmq_assertmsg (r != -1,"msg close part");
      
      break;
    }
  }

  r = zmq_msg_close (&msg_key);
  zmq_assertmsg (r != -1,"msg close key");

 error:

  return 1;
}


int main (int argc, char **argv) 
{

 void *argtable[] = {
   test = arg_int0(NULL,"test","test","test the throughput speed"),
   ntimes = arg_int0("n","tries","tries","number of tries"),
   timeout = arg_int0("t","timeout","timeout","timeout per request, in seconds"),
   blocking = arg_lit0("b","blocking","use blocking mode rather than timeout default"),
    help = arg_lit0("h","help","print this screen"),
    verbose = arg_lit0("v","verbose","tell me everything"),
    key = arg_str1(NULL,NULL,"key","data key"),
    netaddress0 = arg_file1(NULL,NULL,"address","tmpcache read network address "),
    
    end = arg_end(20),
  };
  
  int32_t nerrors = arg_parse(argc,argv,argtable);
  
  if (help->count) {
    
    fprintf(stdout,"tmpcache readfrom - version 0\n");
    arg_print_syntaxv(stdout,argtable,"\n\n");
    arg_print_glossary (stdout,argtable,"%-25s %s\n");
   
    goto finish;
  }

  if (verbose->count) {

    fprintf(stdout,"tmpcache readfrom - version 0\n");
    int32_t major,minor,patch;
    zmq_version (&major,&minor,&patch);
    fprintf(stdout,"compiled with zmq support %d.%d.%d\n",major,minor,patch);

    goto finish;
  }

  if (nerrors) {
    
    arg_print_errors (stdout,end,"");
    arg_print_syntaxv(stdout,argtable,"\n\n");
    goto finish;
  }
  

  void *ctx, *sock;
  
  ctx = zmq_ctx_new();
  if (ctx == NULL) {

    goto finish;
  }

  sock = zmq_socket (ctx,ZMQ_REQ);
  if (sock == NULL) {

    zmq_ctx_destroy (ctx);
    goto finish;
  }
  
  int32_t r = 0;
  r = zmq_connect (sock,netaddress0->filename[0]);
  zmq_assertmsg (r != -1,"connect failed");
  

  uint32_t throughput = (test->count) ? test->ival[0] : 0;
  
  uint32_t i;
  uint32_t tries = (ntimes->count) ? ntimes->ival[0] : 1;
  uint32_t tout = (timeout->count) ? timeout->ival[0] : 1;

  uint32_t (*readf)(void *,bstring,uint32_t,uint32_t,void *) = lazypriatef;

  if (blocking->count)
    readf = blockingf;

  if (throughput == 0) {

    bstring bkey  = bfromcstr(key->sval[0]);
    
    (*readf)(sock,bkey,tries,tout,NULL);
    
    bdestroy (bkey);

  } else { /* do throughput test */

    info_t info; 
    info.numof = 0;
    info.misses = 0;
    info.smallest = 0; /*FIXME*/
    info.largest = 0;
    
    for (i=0; i < throughput; i++) {
      
      info.numof ++;

      bstring bkey = bformat("%s%d",key->sval[0],i);
      (*readf)(sock,bkey,tries,tout,&info);
      bdestroy (bkey);
    }

    fprintf(stdout,"%d/%d smallest=%db, largest=%db\n",info.misses,info.numof,info.smallest,info.largest);
  }
  
 error:
  
  r = zmq_disconnect (sock,netaddress0->filename[0]);
  if (r == -1) {
    
    fprintf(stderr,"disconnect failed - %s",zmq_strerror(zmq_errno()));
  }

  r = zmq_close (sock);
  if (r == -1) {

    fprintf(stderr,"close failed - %s",zmq_strerror(zmq_errno()));
  }

  r = zmq_ctx_destroy (ctx);
  if (r == -1) {

    fprintf(stderr,"ctx destroy failed - %s",zmq_strerror(zmq_errno()));
  }

 finish:
  arg_freetable(argtable,sizeof(argtable)/sizeof(argtable[0]));
  exit(0);
}
