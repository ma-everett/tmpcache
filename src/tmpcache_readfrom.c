
/* tmpcache/tmpcache_client.c */


#include "../config.h"
#include "utility.h"
#include "cache.h"

#include <zmq.h>
#include <argtable2.h>

struct arg_int *ntimes, *timeout;
struct arg_lit *verbose, *help;
struct arg_str *key;
struct arg_file *netaddress0;
struct arg_end *end;


#define zmq_assertmsg(s,str) if ( ! (s) ) {\
  fprintf(stderr,"%s - %s",(str),zmq_strerror(zmq_errno()));\
  goto error; }

int main (int argc, char **argv) 
{

 void *argtable[] = {
   ntimes = arg_int0("n","tries","tries","number of tries"),
   timeout = arg_int0("t","timeout","timeout","timeout per request, in seconds"),
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
  
  zmq_msg_t msg_key;
  void *dkey = c_malloc (sizeof(char) * strlen(key->sval[0]), NULL);
  memcpy (dkey,key->sval[0],strlen(key->sval[0]));
  r = zmq_msg_init_data (&msg_key,dkey,strlen(key->sval[0]),c_free,NULL);
  zmq_assertmsg (r != -1,"init data failed");

  r = zmq_sendmsg (sock,&msg_key,0);
  zmq_assertmsg (r != -1,"sendmsg");

  zmq_pollitem_t pitems[1];
  pitems[0].socket = sock;
  pitems[0].events = ZMQ_POLLIN;

  uint32_t i;
  uint32_t tries = 1;
  if (ntimes->count)
    tries = ntimes->ival[0];

  uint32_t tout = 1;
  if (timeout->count)
    tout = timeout->ival[0];

  for (i = 0; i < tries; i++) {

    uint32_t count = zmq_poll (pitems,1,(1000 * tout));
  
    if (count) {
      
      zmq_msg_t msg_part;
      zmq_msg_init (&msg_part);
      
      r = zmq_recvmsg (sock,&msg_key,0);
      zmq_assertmsg (r != -1,"recvmsg key");
      r = zmq_recvmsg (sock,&msg_part,0);
      zmq_assertmsg (r != -1,"recvmsg part");
      
      if (zmq_msg_size (&msg_part)) {
	
	fputs(zmq_msg_data (&msg_part),stdout);
	fprintf(stdout,"\n");
      }

      r = zmq_msg_close (&msg_part);
      zmq_assertmsg (r != -1,"close msg part");
      break;
    }
  }

  r = zmq_msg_close (&msg_key);
  zmq_assertmsg (r != -1,"close msg key");

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
