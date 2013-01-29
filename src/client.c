
/* tmpcache/client.c 
 */
#include "../config.h"
#include "bstrlib/bstrlib.h"

#include <stdlib.h>
#include <stdio.h> 
#include <argp.h>

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
  {"address",    'a', "ADDR", 0, "Address for cache"},
  {0}
};

typedef struct arguments {

  unsigned int mode;
  bstring address;
  bstring key;
  long long size;

} arguments_t;

long long str2bytes (const char *str) 
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
    
    unsigned char c = toupper ((unsigned char)*bp);
       
    if (c == 'B') {
      d = n * (long long)1024;
      break;
    }

    if (c == 'M') {
      d = n * (long long)1024 * 1024;
      break;
    }

    if (c == 'G') {
      d = n * (long long)(1024 * 1024) * 1024;
      break;
    }
  }
      
  return d;
}
static error_t parseoptions (int key, char *arg, struct argp_state *state)
{
  arguments_t *arguments = state->input;

  switch(key) {
  case 's' :
    arguments->size = arg ? str2bytes(arg) : arguments->size;
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
    if (arg)
      arguments->address = bfromcstr(arg); 
    else
      argp_error(state,"require service address");
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
    fprintf(stderr,"%s! %s\n",__FUNCTION__,xs_strerror(xs_errno()));	\
    goto error;}

void xs_free (void *p, void *hint) { free (p); }

int main (int argc, char **argv) 
{
  arguments_t options;
  options.mode = 0; /* read */
  options.size = 1024 * 1024;
 
  argp_parse (&argp,argc,argv,0,0,&options);

  if (!blength(options.address) || !blength(options.key))
    exit(0);

  int r = 0;
  int interface = 0;
  void *sock = NULL;
  void *ctx = xs_init();
  if (!ctx) {

    fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
    exit(1);
  }

  /* read : */
  if (options.mode == 0) {
    
    sock = xs_socket (ctx,XS_REQ);
    if (!sock) {

      fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
      goto error2;
    }

    r = xs_connect (sock,(char *)options.address->data);
    xs_assert (r != -1);
    
    interface = r;

    xs_msg_t msg_key;
    char *key = malloc(sizeof(char) * blength(options.key));
    if (key == NULL) {

      perror("Failed to allocate memory for key");
      goto error;
    }
    memcpy (key,(char *)options.key->data,blength(options.key));
    xs_msg_init_data (&msg_key,key,blength(options.key),xs_free,NULL);

    xs_msg_t msg_part;
    xs_msg_init (&msg_part);

    r = xs_sendmsg (sock,&msg_key,0);
    xs_assert(r != -1);

    xs_pollitem_t pitems[1];
    pitems[0].socket = sock;
    pitems[0].events = XS_POLLIN;

    int count = xs_poll (pitems,1,(1000 * 5)); /* 5 second default FIXME */
    
    if (count) {

      r = xs_recvmsg (sock,&msg_key,0);
      xs_assert(r != -1);
      
      r = xs_recvmsg (sock,&msg_part,0);
      xs_assert(r != -1);
      
      if (xs_msg_size(&msg_part)) {

	puts(xs_msg_data(&msg_part));
      }
    }

    xs_msg_close (&msg_key);
    xs_msg_close (&msg_part);

    xs_shutdown (sock,interface);
  } 

  /* write : */
  if (options.mode == 1 || options.mode == 2) {

    sock = xs_socket (ctx,XS_PUSH);
    if (!sock) {

      fprintf(stderr,"%s\n",xs_strerror(xs_errno()));
      goto error2;
    }

    r = xs_connect (sock,(char *)options.address->data);
    xs_assert (r != -1);

    interface = r;
	
    xs_msg_t msg_key;
    void *key = malloc(sizeof(char) * blength(options.key));
    if (key == NULL) {

      perror("Failed to allocate memory for key");
      goto error;
    }

    memcpy (key,(char *)options.key->data,blength(options.key));
    xs_msg_init_data (&msg_key,key,blength(options.key),xs_free,NULL);

    xs_msg_t msg_part;

    if (options.mode == 2) {
      
      xs_msg_init (&msg_part);

    } else {
      
      char *buffer = malloc(sizeof(char) * options.size);
      if (buffer == NULL){
	
	perror("Failed to allocate memory for buffer");
	goto error;
      }
      
      memset (buffer,'\0',options.size);
      fgets(buffer,options.size,stdin);
      xs_msg_init_data (&msg_part,buffer,strlen(buffer) - 1,xs_free,NULL);  
    }

    r = xs_sendmsg (sock,&msg_key,XS_SNDMORE);
    xs_assert (r != -1);

    r = xs_sendmsg (sock,&msg_part,0);
    xs_assert (r != -1);   

    xs_msg_close (&msg_key);
    xs_msg_close (&msg_part);

    xs_shutdown (sock,interface);
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
