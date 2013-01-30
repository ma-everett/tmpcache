
/* tmpcache/client.c 
 */
#include "../config.h"
#include "bstrlib/bstrlib.h"
#define btocstr(bs) (char *)(bs)->data

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
  {0}
};

typedef struct arguments {

  unsigned int mode;
  bstring address;
  bstring key;
  long long size;
  unsigned int timeout;

} arguments_t;

long long strtobytes (const char *str) 
{
  char buffer[256];
  memset(&buffer[0],'\0',256);

  int n;
  long long d = 1024;
  int r = sscanf(str,"%d%s",&n,&buffer[0]);
  if (r != 2)
    return 0;

  char *bp = &buffer[0];
  while (*bp != '\0') {
    
    if (*bp == 'B' || *bp == 'b') {
      d = n * (long long)1024;
      break;
    }
    if (*bp == 'M' || *bp == 'm') {
      d = n * (long long)1024 * 1024;
      break;
    }
    if (*bp == 'G' || *bp == 'g') {
      d = n * (long long)(1024 * 1024) * 1024;
      break;
    }
  }
      
  return d;
}

int validaddress (bstring address) 
{
  /* ipc or tcp only connections, so ipc://validpath or tcp://0.0.0.0:5555 */
  if (blength(address) <= 6)
    return 0;

  bstring protocol = bmidstr(address,0,6);
   
  if (biseq (protocol, bfromcstr("ipc://")) == 1) {
   
    bdestroy(protocol);

    bstring path = bmidstr(address,6,blength(address) - 6);
  
    struct stat fstat;
    if (stat(btocstr(path),&fstat) != 0) {
      bdestroy (path);
      return 0;
    }

    bdestroy (path);
    return 1;
  }

  if (biseq (protocol,bfromcstr("tcp://")) == 1) {

    bdestroy (protocol);

    /* FIXME: improve */
    
    return 1;
  }   
  
  bdestroy (protocol);
  return 0;
}

static error_t parseoptions (int key, char *arg, struct argp_state *state)
{
  arguments_t *arguments = state->input;

  switch(key) {
  case 's' :
    arguments->size = arg ? strtobytes(arg) : arguments->size;
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
      if (!validaddress(arguments->address))
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

void xs_free (void *p, void *hint) { free (p); }

int main (int argc, char **argv) 
{
  arguments_t options;
  options.mode = 0; /* read */
  options.size = 1024 * 1024;
  options.timeout = 5;
 
  if (argp_parse (&argp,argc,argv,0,0,&options) != 0) {

    perror("Failed to parse options");
    exit(1);
  }

  if (!blength(options.address) || !blength(options.key))
    exit(1);

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
    xs_assert (r != -1);
    
    interface = r;

    xs_msg_t msg_key;
    char *key = malloc(sizeof(char) * blength(options.key));
    if (key == NULL) {

      perror("Failed to allocate memory for key");
      goto error;
    }
    memcpy (key,btocstr(options.key),blength(options.key));
    r = xs_msg_init_data (&msg_key,key,blength(options.key),xs_free,NULL);
    xs_assert (r != -1);

    xs_msg_t msg_part;
    r = xs_msg_init (&msg_part);
    xs_assert (r != -1);

    r = xs_sendmsg (sock,&msg_key,0);
    xs_assert (r != -1);

    xs_pollitem_t pitems[1];
    pitems[0].socket = sock;
    pitems[0].events = XS_POLLIN;

    int count = xs_poll (pitems,1,(1000 * options.timeout)); 
    
    if (count) {

      r = xs_recvmsg (sock,&msg_key,0);
      xs_assert (r != -1);
      
      r = xs_recvmsg (sock,&msg_part,0);
      xs_assert (r != -1);
      
      if (xs_msg_size(&msg_part)) {

	puts (xs_msg_data(&msg_part));
      }
    
      r = xs_msg_close (&msg_key);
      xs_assert (r != -1);
      r = xs_msg_close (&msg_part);
      xs_assert (r != -1);
    }

    r = xs_shutdown (sock,interface);
    xs_assert (r != -1);
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
    xs_assert (r != -1);

    interface = r;
	
    xs_msg_t msg_key;
    void *key = malloc(sizeof(char) * blength(options.key));
    if (key == NULL) {

      perror("Failed to allocate memory for key");
      goto error;
    }

    memcpy (key,btocstr(options.key),blength(options.key));
    r = xs_msg_init_data (&msg_key,key,blength(options.key),xs_free,NULL);
    xs_assert (r != -1);

    xs_msg_t msg_part;

    if (options.mode == 2) {
      
      r = xs_msg_init (&msg_part);
      xs_assert (r != -1);

    } else {
      
      char *buffer = malloc(sizeof(char) * options.size);
      if (buffer == NULL){
	
	perror("Failed to allocate memory for buffer");
	goto error;
      }
      
      memset (buffer,'\0',options.size);
      if (fgets (buffer,options.size,stdin) == NULL){

	perror("Failed to draw from stdin");
	goto error;
      }

      r = xs_msg_init_data (&msg_part,buffer,strlen(buffer) - 1,xs_free,NULL);  
      xs_assert (r != -1);
    }

    r = xs_sendmsg (sock,&msg_key,XS_SNDMORE);
    xs_assert (r != -1);

    r = xs_sendmsg (sock,&msg_part,0);
    xs_assert (r != -1);   

    r = xs_msg_close (&msg_key);
    xs_assert (r != -1);
    r = xs_msg_close (&msg_part);
    xs_assert (r != -1);

    r = xs_shutdown (sock,interface);
    xs_assert(r != -1);
  }

 error:
  bdestroy (options.key);
  bdestroy (options.address);

  r = xs_close (sock);
  if (r != -1) {
    
    fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
  }

 error2:
  r = xs_term (ctx);
  if (r != -1) {

    fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
  }

  exit(0);
}
