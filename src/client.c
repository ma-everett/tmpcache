
/* tmpcache/client.c 
 */
#include "../config.h"
#include "utility.h"

#include <stdlib.h>
#include <stdio.h> 
#include <argp.h>

#include <unistd.h>
#include <sys/stat.h>

#include <xs/xs.h>

const char *argp_program_version = PACKAGE_STRING;
const char *argp_program_bugaddress = PACKAGE_BUGREPORT;

static char argp_doc[] = "tmpcache, a filesystem cache using message passing"; 
static char args_doc[] = ""; /*FIXME*/

static struct argp_option argp_options[] = {
  {"read",      'r', "key",  0, "Read key from cache"},
  {"write",     'w', "key",  0, "Write key and value to cache"},
  {"delete",    'd', "key",  0, "Delete key from cache"},
  {"size",      's', "1Mb",  0, "Max data size"},
  {"timeout",   't',  "seconds",0,"Request timeout in seconds"},
  {"address",    'a', "ADDR", 0, "Address for cache"},
  {"number",     'n', "N amount",0,"Write|Read N times"},
  {0}
};

typedef struct arguments {

  unsigned int mode;
  bstring address;
  bstring key;
  long long size;
  unsigned int timeout;
  unsigned int number; /* for testing */

} arguments_t;


static error_t parseoptions (int key, char *arg, struct argp_state *state)
{
  arguments_t *arguments = state->input;

  switch(key) {
  case 'n' :
    arguments->number = arg ? atoi(arg) : arguments->size;
    break;
  case 's' :
    arguments->size = arg ? tc_strtobytecount(bfromcstr(arg)) : arguments->size;
    break;
  case 'r' :
    arguments->key = bfromcstr(arg);
    arguments->mode = 0;
    break;
  case 'w' :
    arguments->key = bfromcstr(arg);
    arguments->mode = 1;
    break;
  case 'd' :
    arguments->key = bfromcstr(arg);
    arguments->mode = 2;
    break;
  case 'a' :
    if (arg) {

      arguments->address = bfromcstr(arg);
      if (!tc_validaddress(arguments->address))
	argp_error(state,"improper service address");

    } else {
      argp_error(state,"require service address");
    }
    break;
  case 't' :
    arguments->timeout = atoi(arg);
    if (!arguments->timeout)
      argp_error(state,"improper timeout value");
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

static struct argp argp = {argp_options,parseoptions,args_doc,argp_doc};

#define xs_assert(s) if (!(s)) {\
    fprintf(stderr,"%s\n",xs_strerror(xs_errno()));	\
    goto error;}

#define xs_assertmsg(s,str) if (!(s)) {			     \
  fprintf(stderr,"%s - %s\n",(str),xs_strerror(xs_errno())); \
  goto error;}

void xs_free (void *p, void *hint) { free (p); }

int main (int argc, char **argv) 
{
  arguments_t options;
  options.mode = 0; /* read */
  options.size = 1024 * 1024;
  options.timeout = 5;
  options.number = 0;
 
  if (argp_parse (&argp,argc,argv,0,0,&options) != 0) {

    perror("Failed to parse options");
    exit(1);
  }

  if (!blength(options.address) || !blength(options.key))
    exit(1);

  if (!tc_checkaddress(options.address)) {
    
    fprintf(stderr,"invalid network address\n");
    exit(2);
  }

  int r = 0;
  int interface = 0;
  void *sock = NULL;
  void *ctx = xs_init();
  if (!ctx) {

    fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
    exit(2);
  }

  /* Perform a read operation, we ask the service on the other
   * end of the address passed for the value of the key using
   * crossroads.io library. We wait for a reply within the 
   * timeout period and then put to stdout. 
   */
  if (options.mode == 0) {
        
    sock = xs_socket (ctx,XS_REQ);
    if (!sock) {

      fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
      goto error2;
    }

    r = xs_connect (sock,btocstr(options.address));
    xs_assertmsg ((r != -1),"xs connection error");
    
    interface = r;

    xs_msg_t msg_key;
    char *key = malloc(sizeof(char) * blength(options.key));
    if (key == NULL) {

      perror("Failed to allocate memory for key");
      goto error;
    }
    memcpy (key,btocstr(options.key),blength(options.key));
    r = xs_msg_init_data (&msg_key,key,blength(options.key),xs_free,NULL);
    xs_assertmsg ((r != -1),"xs key init error");
    
    xs_pollitem_t pitems[1];
    pitems[0].socket = sock;
    pitems[0].events = XS_POLLIN;
    
    if (options.number == 0) {

      xs_msg_t msg_part;
      r = xs_msg_init (&msg_part);
      xs_assertmsg ((r != -1),"xs part init error");
      
      r = xs_sendmsg (sock,&msg_key,0);
      xs_assertmsg ((r != -1),"xs sendmsg error");
      
      int count = xs_poll (pitems,1,(1000 * options.timeout)); 
    
      if (count) {
	
	r = xs_recvmsg (sock,&msg_key,0);
	xs_assertmsg ((r != -1),"xs recvmsg error");
	
	r = xs_recvmsg (sock,&msg_part,0);
	xs_assertmsg ((r != -1),"xs recvmsg error");
	
	if (xs_msg_size(&msg_part)) {
	  
	  puts (xs_msg_data(&msg_part));
	}
	
	r = xs_msg_close (&msg_key);
	xs_assertmsg ((r != -1),"xs msg close error");
	r = xs_msg_close (&msg_part);
	xs_assertmsg ((r != -1),"xs msg close error");
      }
    } else { /* test version */
      
      xs_msg_t msg_key_c;
      int i;

      xs_msg_t msg_part;

      unsigned int misses = 0;
      
      for (i = 0; i < options.number; i++) {

	xs_msg_init (&msg_key_c);
	xs_msg_copy (&msg_key_c,&msg_key);
	
	xs_msg_init (&msg_part);

	r = xs_sendmsg (sock,&msg_key_c,0);
	xs_assertmsg ((r != -1),"xs sendmsg error");

	int count = xs_poll (pitems,1,(1000 * options.timeout));

	if (count == 0) /* timeout error */
	  break;

	r = xs_recvmsg (sock,&msg_key_c,0);
	xs_assertmsg ((r != -1),"xs recvmsg error");
	
	r = xs_recvmsg (sock,&msg_part,0);
	xs_assertmsg ((r != -1),"xs recvmsg error");
	
	misses += (xs_msg_size(&msg_part) == 0) ? 1 : 0;
      }

      fprintf(stdout,"misses %d/%d\n",misses,options.number);

      r = xs_msg_close (&msg_key_c);
      xs_assertmsg ((r != -1),"xs msg close error");
      r = xs_msg_close (&msg_part);
      xs_assertmsg ((r != -1),"xs msg close error");
      r = xs_msg_close (&msg_key);
      xs_assertmsg ((r != -1),"xs msg close error");
    }

    r = xs_shutdown (sock,interface);
    xs_assertmsg ((r != -1),"xs sock shutdown error");
  } 

  /* Perform a write|delete operation, the service does not
   * acknowlegde our message to write or delete so we don't
   * need to wait. For write we read in from stdin first.
   */
  if (options.mode == 1 || options.mode == 2) {

    sock = xs_socket (ctx,XS_PUSH);
    if (!sock) {

      fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
      goto error2;
    }

    r = xs_connect (sock,btocstr(options.address));
    xs_assertmsg ((r != -1),"xs connect error");

    interface = r;
	
    xs_msg_t msg_key;
    void *key = malloc(sizeof(char) * blength(options.key));
    if (key == NULL) {

      perror("Failed to allocate memory for key");
      goto error;
    }

    memcpy (key,btocstr(options.key),blength(options.key));
    r = xs_msg_init_data (&msg_key,key,blength(options.key),xs_free,NULL);
    xs_assertmsg ((r != -1),"xs key init error");

    xs_msg_t msg_part;

    if (options.mode == 2) {
      
      r = xs_msg_init (&msg_part);
      xs_assertmsg ((r != -1),"xs part init error");

    } else {
      
      char *buffer = malloc (sizeof(char) * options.size);
      if (buffer == NULL){
	
	perror("Failed to allocate memory for buffer");
	goto error;
      }
      
      memset (buffer,'\0',options.size);
      if (fgets (buffer,options.size,stdin) == NULL){

	perror("Failed to get from stdin");
	goto error;
      }

      char *data = malloc (sizeof(char) * strlen(buffer) - 1);
      memcpy (data,buffer,strlen(buffer) - 1);
      r = xs_msg_init_data (&msg_part,data,strlen(buffer) - 1,xs_free,NULL);  
      xs_assertmsg ((r != -1),"xs part init error");

      free (buffer);
    }

    r = xs_sendmsg (sock,&msg_key,XS_SNDMORE);
    xs_assertmsg ((r != -1),"xs sendmsg error");

    r = xs_sendmsg (sock,&msg_part,0);
    xs_assertmsg ((r != -1),"xs sendmsg error");   

    r = xs_msg_close (&msg_key);
    xs_assertmsg ((r != -1),"xs msg close error");
    r = xs_msg_close (&msg_part);
    xs_assertmsg ((r != -1),"xs msg close error");

    r = xs_shutdown (sock,interface);
    xs_assertmsg ((r != -1),"xs sock shutdown error");
  }

 error:
  bdestroy (options.key);
  bdestroy (options.address);

  r = xs_close (sock);
  if (r != 0) {
    
    fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
  }

 error2:
  r = xs_term (ctx);
  if (r != 0) {

    fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
  }

  exit(0);
}
