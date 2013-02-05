
/* tmpcache/tmpcache_writeto.c */


#include "../config.h"
#include "utility.h"
#include "cache.h"

#include <zmq.h>
#include <argtable2.h>

struct arg_lit *delete;
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
   delete = arg_lit0("d","delete","delete stored data"),
    help = arg_lit0("h","help","print this screen"),
    verbose = arg_lit0("v","verbose","tell me everything"),
    key = arg_str1(NULL,NULL,"key","data key"),
    netaddress0 = arg_file1(NULL,NULL,"address","tmpcache write network address "),
    
    end = arg_end(20),
  };
  
  int32_t nerrors = arg_parse(argc,argv,argtable);
  
  if (help->count) {
    
    fprintf(stdout,"tmpcache writeto - version 0\n"); /*FIXME*/
    arg_print_syntaxv(stdout,argtable,"\n\n");
    arg_print_glossary (stdout,argtable,"%-25s %s\n");
   
    goto finish;
  }

  if (verbose->count) {

    fprintf(stdout,"tmpcache writeto - version 0\n"); /*FIXME*/
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

  sock = zmq_socket (ctx,ZMQ_PUSH);
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

  zmq_msg_t msg_part;

  if (!delete->count) {

    char *buffer = c_malloc (sizeof(char) * (1024 * 1024),NULL); /* 1Mb */
    
    if (buffer == NULL){
      
      perror("Failed to allocate memory for buffer");
      goto error;
    }
    
    memset (buffer,'\0',(1024 * 1024));
    if (fgets (buffer,(1024 * 1024),stdin) == NULL){
      
      perror("Failed to get from stdin");
      goto error;
    }
    
    uint32_t len = strlen(buffer) - 1;
    
    char *data = c_malloc (sizeof(char) * len,NULL);
    memcpy (data,buffer,len);
    c_free (buffer,NULL);
       
    r = zmq_msg_init_data (&msg_part,data,len,c_free,NULL);  
    zmq_assertmsg ((r != -1),"part init error");
  } else {

    r = zmq_msg_init (&msg_part);
    zmq_assertmsg ((r != -1),"part init error");
  }

  r = zmq_sendmsg (sock,&msg_key,ZMQ_SNDMORE);
  zmq_assertmsg (r != -1,"sendmsg key");
  
  r = zmq_sendmsg (sock,&msg_part,0);
  zmq_assertmsg (r != -1,"sendmsg part");
  

  r = zmq_msg_close (&msg_key);
  zmq_assertmsg (r != -1,"msg close key");

  r = zmq_msg_close (&msg_part);
  zmq_assertmsg (r != -1,"msg close part");

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
